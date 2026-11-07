################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
################################################################################
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from pyflink.fn_execution._worker_profiling import process_identity, register_worker
from service import BusyError, ProfilingService, make_server


class ProfilingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.registry = self.root / 'workers'
        self.executable = self.root / 'sampler'
        self.service = ProfilingService(self.registry, self.root / 'results', str(self.executable))
        self.environment = patch.dict(os.environ, {
            'PYFLINK_WORKER_PROFILE_DIR': str(self.registry), 'WORKER_ID': 'beam-worker/1'})
        self.environment.start()
        self.registration = register_worker()
        self.registration.__enter__()
        self.worker = self.service.workers()[0]

    def tearDown(self):
        self.service.close()
        self.registration.__exit__(None, None, None)
        self.environment.stop()
        self.temporary.cleanup()

    def sampler(self, body):
        self.executable.write_text('#!%s\n%s\n' % (sys.executable, body))
        self.executable.chmod(0o700)

    def completed(self, profile):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            result = self.service.get(profile['profile_id'])
            if result['status'] != 'RUNNING':
                return result
            time.sleep(0.02)
        self.fail('Profiling session did not reach a terminal state')

    def blocking_sampler(self):
        self.sampler("""import os, pathlib, sys, time
pathlib.Path(sys.argv[sys.argv.index('--output') + 1]).write_text('partial fixture')
pathlib.Path(__file__).with_suffix('.ready').write_text(str(os.getpid()))
time.sleep(30)
""")

    def wait_for_sampler(self):
        ready = self.executable.with_suffix('.ready')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if ready.exists():
                return int(ready.read_text())
            time.sleep(0.02)
        self.fail('Sampler did not start')

    def test_registration_lifetime_and_pid_identity(self):
        self.assertEqual(self.worker['pid'], os.getpid())
        self.assertEqual(self.worker['start_token'], process_identity(os.getpid()))
        self.assertEqual(self.worker['worker_id'], 'beam-worker/1')
        self.registration.__exit__(None, None, None)
        self.assertEqual(self.service.workers(), [])

    def test_disabled_registration_does_not_query_process(self):
        with patch.dict(os.environ, {}, clear=True), patch(
                'pyflink.fn_execution._worker_profiling.process_identity') as identity:
            with register_worker():
                pass
            identity.assert_not_called()

    def test_registration_failure_does_not_stop_worker(self):
        self.registry.joinpath('not-a-directory').write_text('file')
        with patch.dict(os.environ, {
                'PYFLINK_WORKER_PROFILE_DIR': str(self.registry / 'not-a-directory')}):
            with self.assertLogs(level='WARNING'):
                with register_worker():
                    reached_worker = True
            self.assertTrue(reached_worker)

    def test_worker_exception_is_not_swallowed(self):
        with self.assertRaisesRegex(RuntimeError, 'job failure'):
            with register_worker():
                raise RuntimeError('job failure')
        self.assertEqual(len(self.service.workers()), 1)

    def test_changed_start_token_cannot_be_sampled(self):
        path = self.registry / (self.worker['registration_id'] + '.json')
        path.write_text(json.dumps({**self.worker, 'start_token': 'old-incarnation'}))
        self.assertEqual(self.service.workers(), [])
        with self.assertRaises(KeyError):
            self.service.start(self.worker['registration_id'])

    def test_arguments_and_fixture_result(self):
        self.sampler("""import json, pathlib, sys
pathlib.Path(__file__).with_suffix('.args').write_text(json.dumps(sys.argv[1:]))
pathlib.Path(sys.argv[sys.argv.index('--output') + 1]).write_text(
    '<svg>TEST FIXTURE: NOT A SAMPLED PROFILE</svg>')
""")
        profile = self.service.start(self.worker['registration_id'], 2, 25, True)
        result = self.completed(profile)
        self.assertEqual(result['status'], 'SUCCESS')
        args = json.loads(self.executable.with_suffix('.args').read_text())
        self.assertEqual(args[args.index('--pid') + 1], str(os.getpid()))
        self.assertEqual(args[args.index('--duration') + 1], '2')
        self.assertEqual(args[args.index('--rate') + 1], '25')
        self.assertIn('--idle', args)
        self.assertIn(b'TEST FIXTURE', self.service.result(profile['profile_id']))

    def test_permission_failure_is_explicit(self):
        self.sampler("import sys; print('py-spy requires root permissions'); sys.exit(1)")
        profile = self.service.start(self.worker['registration_id'])
        result = self.completed(profile)
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['error_code'], 'PERMISSION_DENIED')
        with self.assertRaises(BusyError):
            self.service.result(profile['profile_id'])

    def test_missing_sampler(self):
        result = self.completed(self.service.start(self.worker['registration_id']))
        self.assertEqual(result['error_code'], 'SAMPLER_NOT_FOUND')

    def test_empty_output_is_not_success(self):
        self.sampler('pass')
        result = self.completed(self.service.start(self.worker['registration_id']))
        self.assertEqual(result['error_code'], 'NO_OUTPUT')

    def test_duplicate_capture_and_cancellation(self):
        self.blocking_sampler()
        profile = self.service.start(self.worker['registration_id'])
        sampler_pid = self.wait_for_sampler()
        with self.assertRaises(BusyError):
            self.service.start(self.worker['registration_id'])
        self.service.cancel(profile['profile_id'])
        self.assertEqual(self.completed(profile)['status'], 'CANCELLED')
        self.assertIsNone(process_identity(sampler_pid))
        self.assertFalse((self.service.output / (profile['profile_id'] + '.svg')).exists())

    def test_sampler_timeout(self):
        self.sampler('import time; time.sleep(30)')
        result = self.completed(self.service.start(self.worker['registration_id'], 1))
        self.assertEqual(result['error_code'], 'TIMEOUT')

    def test_worker_exit_during_capture(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        try:
            worker = {**self.worker, 'registration_id': 'child', 'pid': child.pid,
                      'start_token': process_identity(child.pid)}
            self.registry.joinpath('child.json').write_text(json.dumps(worker))
            self.blocking_sampler()
            profile = self.service.start('child')
            sampler_pid = self.wait_for_sampler()
            child.terminate()
            child.wait(timeout=3)
            self.assertEqual(self.completed(profile)['error_code'], 'WORKER_EXITED')
            self.assertIsNone(process_identity(sampler_pid))
            self.assertFalse((self.service.output / (profile['profile_id'] + '.svg')).exists())
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    def test_cleanup_failure_does_not_leave_session_running(self):
        self.sampler("""import pathlib, sys
output = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])
output.write_text('partial fixture')
output.parent.chmod(0o500)
sys.exit(1)
""")
        try:
            profile = self.service.start(self.worker['registration_id'])
            self.assertEqual(self.completed(profile)['status'], 'FAILED')
        finally:
            self.service.output.chmod(0o700)
        self.sampler('pass')
        next_profile = self.service.start(self.worker['registration_id'])
        self.assertEqual(self.completed(next_profile)['error_code'], 'NO_OUTPUT')

    def test_request_limits(self):
        for duration, rate, idle in ((0, 50, False), (61, 50, False), (True, 50, False),
                                     (10, 101, False), (10, 50, 'true')):
            with self.subTest(duration=duration, rate=rate, idle=idle):
                with self.assertRaises(ValueError):
                    self.service.start(self.worker['registration_id'], duration, rate, idle)

    def test_http_lifecycle_and_download(self):
        self.sampler("""import pathlib, sys
pathlib.Path(sys.argv[sys.argv.index('--output') + 1]).write_text('<svg>TEST FIXTURE</svg>')
""")
        server = make_server(self.service)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        url = 'http://127.0.0.1:%d' % server.server_port
        try:
            with urllib.request.urlopen(url + '/workers') as response:
                workers = json.load(response)
            request = urllib.request.Request(
                url + '/workers/' + workers[0]['registration_id'] + '/profiles',
                data=b'{"duration_seconds": 1}', headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request) as response:
                self.assertEqual(response.status, 202)
                profile = json.load(response)
            self.assertEqual(self.completed(profile)['status'], 'SUCCESS')
            with urllib.request.urlopen(url + '/profiles/' + profile['profile_id']) as response:
                self.assertEqual(json.load(response)['status'], 'SUCCESS')
            download_url = url + '/profiles/' + profile['profile_id'] + '/result'
            with urllib.request.urlopen(download_url) as r:
                self.assertIn('attachment', r.headers['Content-Disposition'])
                self.assertIn(b'TEST FIXTURE', r.read())
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(url + '/profiles/not-registered/result')
            self.assertEqual(error.exception.code, 404)
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()


if __name__ == '__main__':
    unittest.main()
