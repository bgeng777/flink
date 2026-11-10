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
"""One bounded, opt-in Memray capture in the Python worker process."""

import json
import logging
import math
import os
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread
import time
import uuid

from pyflink.fn_execution._worker_profiling import set_memory_profiling_busy

# A failed Tracker close must retain admission until the worker process exits.
_unconfirmed_capture_lock = None


def _publish_capture(registration, profile, stop_confirmed):
    if registration is None:
        return
    record = json.loads(registration.read_text())
    record['profile'] = profile
    record['stop_confirmed'] = stop_confirmed
    directory = registration.parent / 'profiles'
    directory.mkdir(mode=0o700, exist_ok=True)
    target = directory / registration.name
    temporary = target.with_suffix('.tmp')
    try:
        with temporary.open('w') as output:
            os.chmod(temporary, 0o600)
            json.dump(record, output)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def profile_worker_memory(registration=None):
    directory = os.environ.get('PYFLINK_MEMRAY_OUTPUT_DIR')
    if not directory:
        logging.info('Python worker Memray capture is disabled')
        yield
        return

    try:
        if os.environ.get('PYFLINK_MEMRAY_CPROFILE') == 'true':
            raise ValueError('Memray and cProfile cannot run concurrently in the same worker')
        delay = float(os.environ.get('PYFLINK_MEMRAY_DELAY', '0'))
        duration = float(os.environ.get('PYFLINK_MEMRAY_DURATION', '60'))
        if not math.isfinite(delay) or delay < 0:
            raise ValueError('PYFLINK_MEMRAY_DELAY must be finite and non-negative')
        if not math.isfinite(duration) or duration <= 0:
            raise ValueError('PYFLINK_MEMRAY_DURATION must be finite and positive')
        if duration > float(os.environ.get('PYFLINK_MEMRAY_MAX_DURATION', '300')):
            raise ValueError('Memory capture duration exceeds rest.profiling.duration-max')
        native = os.environ.get('PYFLINK_MEMRAY_NATIVE_TRACES', 'false').lower()
        if native not in ('true', 'false'):
            raise ValueError('PYFLINK_MEMRAY_NATIVE_TRACES must be true or false')
        lock_file = os.environ['PYFLINK_PROFILING_LOCK_FILE']

        import fcntl
        from memray import FileDestination, Tracker

        root = Path(directory).resolve()
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        capture = root / ('python-memory-startup-%s-%s.bin' % (os.getpid(), uuid.uuid4().hex))
    except (ImportError, KeyError, OSError, ValueError):
        logging.warning('Could not prepare Python worker Memray capture', exc_info=True)
        yield
        return

    stop = Event()
    ready = Event()

    def record():
        global _unconfirmed_capture_lock
        slot = None
        stop_confirmed = True
        profile = None
        pending = capture.with_suffix('.bin.inprogress')
        try:
            if stop.wait(delay):
                logging.info('Python worker Memray capture cancelled before starting: %s', capture)
                return
            slot = open(lock_file, 'a+b')
            try:
                # lockf uses the same POSIX record locks as Java FileChannel.tryLock.
                fcntl.lockf(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                logging.info('Skipping Python worker Memray capture: the Flink JVM is profiling')
                return
            set_memory_profiling_busy(registration, True)
            profile = {
                'status': 'RUNNING', 'type': 'MEMORY',
                'triggerTime': time.time_ns() // 1_000_000,
                'duration': math.ceil(duration), 'includeNative': native == 'true',
            }
            _publish_capture(registration, profile, False)
            tracker = Tracker(destination=FileDestination(pending, compress_on_exit=False),
                              native_traces=native == 'true')
            stop_confirmed = False
            with tracker:
                logging.info('Python worker Memray capture started: pid=%s, started_at=%s, file=%s',
                             os.getpid(), time.time(), pending)
                ready.set()
                stop.wait(duration)
            stop_confirmed = True
            pending.replace(capture)
            profile.update(status='FINISHED', finishedTime=time.time_ns() // 1_000_000,
                           outputFile=capture.name)
            logging.info('Python worker Memray capture completed: pid=%s, ended_at=%s, file=%s',
                         os.getpid(), time.time(), capture)
        except Exception as error:
            # Profiler failures must not replace an exception raised by the harness.
            logging.warning('Python worker Memray capture failed: %s', capture, exc_info=True)
            if profile is not None:
                message = str(error)
                if not stop_confirmed:
                    message += ('; stop unconfirmed; profiling remains reserved '
                                'until the worker exits')
                profile.update(status='FAILED', finishedTime=time.time_ns() // 1_000_000,
                               message=message)
        finally:
            try:
                if profile is not None:
                    _publish_capture(registration, profile, stop_confirmed)
            except OSError:
                logging.warning('Could not publish Python worker Memray status', exc_info=True)
            finally:
                if stop_confirmed:
                    set_memory_profiling_busy(registration, False)
                    if slot is not None:
                        slot.close()
                else:
                    _unconfirmed_capture_lock = slot
                ready.set()

    logging.info('Python worker Memray capture enabled: pid=%s, delay=%ss, duration=%ss, '
                 'native_traces=%s, file=%s', os.getpid(), delay, duration, native, capture)
    recorder = Thread(target=record, name='pyflink-memray', daemon=True)
    try:
        recorder.start()
    except RuntimeError:
        logging.warning('Could not start Python worker Memray capture', exc_info=True)
        yield
        return

    try:
        if delay == 0:
            ready.wait()
        yield
    finally:
        stop.set()
        recorder.join()
