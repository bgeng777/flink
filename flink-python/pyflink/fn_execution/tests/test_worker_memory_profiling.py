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

from contextlib import ExitStack
import os
from pathlib import Path
import sys
import tempfile
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pyflink.fn_execution import _worker_memory_profiling as profiling


class WorkerMemoryProfilingTests(unittest.TestCase):
    def setUp(self):
        self.contexts = ExitStack()
        self.addCleanup(self.contexts.close)
        directory = self.contexts.enter_context(tempfile.TemporaryDirectory())
        self.output = Path(directory) / 'profiles'
        self.contexts.enter_context(patch.dict(os.environ, {
            'PYFLINK_MEMRAY_OUTPUT_DIR': str(self.output),
            'PYFLINK_MEMRAY_DELAY': '0',
            'PYFLINK_MEMRAY_DURATION': '3600',
            'PYFLINK_MEMRAY_NATIVE_TRACES': 'false',
        }))
        self.entered = Event()
        self.closed = Event()
        self.exit_count = 0
        self.native_traces = None
        self.enter_error = None
        self.exit_error = None
        owner = self

        class FileDestination:
            def __init__(self, path, compress_on_exit):
                self.path = path
                owner.assertFalse(compress_on_exit)

        class Tracker:
            def __init__(self, destination, native_traces):
                self.path = destination.path
                owner.native_traces = native_traces

            def __enter__(self):
                self.path.write_bytes(b'recording')
                if owner.enter_error:
                    raise owner.enter_error
                owner.entered.set()
                return self

            def __exit__(self, *exc):
                owner.exit_count += 1
                if owner.exit_error:
                    raise owner.exit_error
                self.path.write_bytes(b'closed')
                owner.closed.set()

        self.contexts.enter_context(patch.dict(sys.modules, {
            'memray': SimpleNamespace(Tracker=Tracker, FileDestination=FileDestination),
        }))

    def test_disabled_does_not_import_memray_or_start_thread(self):
        os.environ.pop('PYFLINK_MEMRAY_OUTPUT_DIR')
        with patch.dict(sys.modules, {'memray': None}), patch.object(profiling, 'Thread') as thread:
            with self.assertLogs(level='INFO') as logs:
                with profiling.profile_worker_memory():
                    pass
        thread.assert_not_called()
        self.assertFalse(self.output.exists())
        self.assertIn('capture is disabled', '\n'.join(logs.output))

    def test_immediate_capture_closes_on_worker_exit(self):
        with self.assertLogs(level='INFO') as logs:
            with profiling.profile_worker_memory():
                self.assertTrue(self.entered.is_set())
                self.assertEqual([], list(self.output.glob('*.bin')))
                self.assertEqual(1, len(list(self.output.glob('*.inprogress'))))
        self.assertEqual(1, self.exit_count)
        captures = list(self.output.glob('*.bin'))
        self.assertEqual(1, len(captures))
        self.assertEqual(b'closed', captures[0].read_bytes())
        self.assertIn('worker-%s-' % os.getpid(), captures[0].name)
        self.assertFalse(list(self.output.glob('*.inprogress')))
        self.assertFalse(self.native_traces)
        output = '\n'.join(logs.output)
        self.assertIn('started_at=', output)
        self.assertIn('ended_at=', output)

    def test_worker_exception_is_propagated_after_capture_closes(self):
        failure = ValueError('user function failed')
        with self.assertRaises(ValueError) as raised:
            with profiling.profile_worker_memory():
                raise failure
        self.assertIs(failure, raised.exception)
        self.assertTrue(self.closed.is_set())
        self.assertEqual(1, self.exit_count)

    def test_worker_exiting_before_delay_cancels_capture(self):
        os.environ['PYFLINK_MEMRAY_DELAY'] = '3600'
        with self.assertLogs(level='INFO') as logs:
            with profiling.profile_worker_memory():
                self.assertFalse(self.entered.is_set())
        self.assertFalse(self.entered.is_set())
        self.assertEqual([], list(self.output.iterdir()))
        self.assertIn('cancelled before starting', '\n'.join(logs.output))

    def test_delayed_capture_starts_while_worker_is_running(self):
        os.environ['PYFLINK_MEMRAY_DELAY'] = '120'
        delay_elapsed = Event()
        stopped = Event()

        def wait(timeout):
            if timeout == 120:
                if not delay_elapsed.wait(5):
                    raise TimeoutError('Test did not release the delayed capture')
                return stopped.is_set()
            return stopped.wait(timeout)

        stop = Mock(wait=Mock(side_effect=wait), set=stopped.set)
        with patch.object(profiling, 'Event', side_effect=[stop, Event()]):
            with profiling.profile_worker_memory():
                self.assertFalse(self.entered.is_set())
                delay_elapsed.set()
                self.assertTrue(self.entered.wait(5))
                self.assertFalse(self.closed.is_set())
        self.assertTrue(self.closed.is_set())
        self.assertEqual(1, self.exit_count)

    def test_timed_capture_finishes_without_stopping_worker(self):
        # Advance both waits without relying on elapsed wall-clock time.
        stop = Mock(wait=Mock(return_value=False))
        with patch.object(profiling, 'Event', side_effect=[stop, Event()]):
            with profiling.profile_worker_memory():
                self.assertTrue(self.closed.wait(5))
                self.assertEqual(1, self.exit_count)
        self.assertEqual(1, self.exit_count)
        self.assertEqual(1, len(list(self.output.glob('*.bin'))))

    def test_native_capture(self):
        os.environ['PYFLINK_MEMRAY_NATIVE_TRACES'] = 'true'
        with profiling.profile_worker_memory():
            self.assertTrue(self.native_traces)

    def test_repeated_startup_uses_distinct_output_files(self):
        for _ in range(2):
            with profiling.profile_worker_memory():
                pass
        self.assertEqual(2, len(list(self.output.glob('*.bin'))))

    def test_missing_memray_preserves_worker_startup(self):
        with patch.dict(sys.modules, {'memray': None}), self.assertLogs(level='WARNING') as logs:
            with profiling.profile_worker_memory():
                self.assertFalse(self.entered.is_set())
        self.assertFalse(self.output.exists())
        self.assertIn('Could not prepare', '\n'.join(logs.output))

    def test_invalid_options_preserve_worker_startup(self):
        invalid = [
            ('PYFLINK_MEMRAY_DELAY', '-1'),
            ('PYFLINK_MEMRAY_DELAY', 'nan'),
            ('PYFLINK_MEMRAY_DELAY', 'inf'),
            ('PYFLINK_MEMRAY_DURATION', '0'),
            ('PYFLINK_MEMRAY_DURATION', '-1'),
            ('PYFLINK_MEMRAY_DURATION', 'nan'),
            ('PYFLINK_MEMRAY_DURATION', 'inf'),
            ('PYFLINK_MEMRAY_DURATION', 'invalid'),
            ('PYFLINK_MEMRAY_NATIVE_TRACES', 'invalid'),
        ]
        for key, value in invalid:
            with self.subTest(key=key, value=value), patch.dict(os.environ, {key: value}):
                with self.assertLogs(level='WARNING') as logs:
                    with profiling.profile_worker_memory():
                        self.assertFalse(self.entered.is_set())
                self.assertIn('Could not prepare', '\n'.join(logs.output))
        self.assertFalse(self.output.exists())

    def test_directory_error_preserves_worker_startup(self):
        self.output.write_text('existing file')
        with self.assertLogs(level='WARNING') as logs:
            with profiling.profile_worker_memory():
                self.assertFalse(self.entered.is_set())
        self.assertEqual('existing file', self.output.read_text())
        self.assertIn('Could not prepare', '\n'.join(logs.output))

    def test_tracker_start_failure_preserves_worker_startup(self):
        self.enter_error = RuntimeError('another tracker is active')
        with self.assertLogs(level='WARNING') as logs:
            with profiling.profile_worker_memory():
                self.assertFalse(self.entered.is_set())
        self.assertEqual([], list(self.output.glob('*.bin')))
        self.assertEqual(0, self.exit_count)
        self.assertIn('capture failed', '\n'.join(logs.output))

    def test_tracker_close_failure_does_not_publish_complete_capture(self):
        self.exit_error = OSError('cannot flush capture')
        with self.assertLogs(level='INFO') as logs:
            with profiling.profile_worker_memory():
                pass
        self.assertEqual(1, self.exit_count)
        self.assertEqual([], list(self.output.glob('*.bin')))
        self.assertEqual(1, len(list(self.output.glob('*.inprogress'))))
        self.assertNotIn('capture completed', '\n'.join(logs.output))
        self.assertIn('capture failed', '\n'.join(logs.output))

    def test_thread_start_failure_preserves_worker_startup(self):
        with patch.object(profiling, 'Thread') as thread, self.assertLogs(level='WARNING') as logs:
            thread.return_value.start.side_effect = RuntimeError('cannot start new thread')
            with profiling.profile_worker_memory():
                self.assertFalse(self.entered.is_set())
        self.assertIn('Could not start', '\n'.join(logs.output))


if __name__ == '__main__':
    unittest.main()
