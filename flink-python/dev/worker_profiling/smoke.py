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
"""Exercise the real Flink REST profiling route with a standalone PyFlink worker.

Requires a matching built distribution, PyFlink dependencies, and py-spy on PATH.
Permission failures are recorded independently of the job's success.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

from pyflink.fn_execution._worker_profiling import process_identity


def main(directory, include_native=False, include_idle=False):
    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    with socket.socket() as socket_probe:
        socket_probe.bind(('127.0.0.1', 0))
        port = socket_probe.getsockname()[1]
    base = 'http://127.0.0.1:%d' % port

    def request(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base + path, data=data,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.load(response)

    executable = shutil.which('py-spy')
    if executable is None:
        raise RuntimeError('Install py-spy in the diagnostic environment first')
    environment = {**os.environ, '_python_worker_execution_mode': 'process',
                   'PYFLINK_PROFILE_REST_PORT': str(port),
                   'PYFLINK_PROFILE_EXECUTABLE': executable,
                   'PYFLINK_PROFILE_OUTPUT': str(root / 'profiles')}
    job = None
    try:
        with root.joinpath('job.log').open('w') as log:
            command = [sys.executable, '-u', str(Path(__file__).with_name('probe_job.py'))]
            job = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 90
            while True:
                try:
                    managers = request('/taskmanagers')['taskmanagers']
                    if managers:
                        prefix = '/taskmanagers/' + managers[0]['id']
                        workers = request(prefix + '/python-workers')['workers']
                        if workers:
                            break
                except urllib.error.URLError:
                    pass  # The MiniCluster REST endpoint is not ready yet.
                if job.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('No worker found through Flink REST; inspect job.log')
                time.sleep(0.1)
            worker = workers[0]
            profile = request(prefix + '/profiler',
                              {'duration': 2, 'workerId': worker['id'],
                               'includeIdle': include_idle, 'includeNative': include_native})
            deadline = time.monotonic() + 15
            while profile['status'] == 'RUNNING':
                if time.monotonic() > deadline:
                    raise RuntimeError('Profiling did not finish')
                time.sleep(0.1)
                profile = request(prefix + '/profiler?target=python')['profilingList'][0]
            assert profile.get('includeNative') is include_native, profile
            assert profile.get('includeIdle') is include_idle, profile
            assert profile['worker']['id'] == worker['id'], profile
            if profile['status'] == 'FINISHED':
                url = base + prefix + '/profiler/' + profile['outputFile']
                with urllib.request.urlopen(url, timeout=10) as r:
                    root.joinpath('worker-flamegraph.svg').write_bytes(r.read())
            job.wait(timeout=60)
        if job.returncode != 0:
            raise RuntimeError('PyFlink job failed; inspect job.log')
        result = next(json.loads(line) for line in root.joinpath('job.log').read_text().splitlines()
                      if line.startswith('{"client_pid"'))
        assert result['client_pid'] != worker['pid'], (result, worker)
        assert {row[0] for row in result['rows']} == {worker['pid']}, (result, worker)
        assert sorted(row[1:] for row in result['rows']) == [
            [value, sum(x * x for x in range(500))] for value in (1, 2, 3, 4)]
        summary = {'backend': 'Flink REST/RM/TM', 'worker': worker, 'profile': profile,
                   'job': result, 'worker_pid_verified': True,
                   'worker_exited': process_identity(worker['pid']) is None}
        root.joinpath('result.json').write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary, indent=2))
    finally:
        if job is not None and job.poll() is None:
            job.terminate()
            try:
                job.wait(timeout=3)
            except subprocess.TimeoutExpired:
                job.kill()
                job.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--native', action='store_true', help='Include native extension stacks')
    parser.add_argument('--idle', action='store_true', help='Include idle threads')
    args = parser.parse_args()
    main(args.directory, args.native, args.idle)
