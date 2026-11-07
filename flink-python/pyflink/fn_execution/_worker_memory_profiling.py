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
"""Experimental startup memory capture; not a supported configuration/API."""

import logging
import math
import os
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread
import time
import uuid

from pyflink.fn_execution._worker_profiling import finish_startup_memory_capture


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

        from memray import FileDestination, Tracker

        root = Path(directory).resolve()
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        capture = root / ('python-memory-startup-%s-%s.bin' % (os.getpid(), uuid.uuid4().hex))
    except (ImportError, OSError, ValueError):
        logging.warning('Could not prepare Python worker Memray capture', exc_info=True)
        finish_startup_memory_capture(registration)
        yield
        return

    stop = Event()
    ready = Event()

    def record():
        try:
            if stop.wait(delay):
                finish_startup_memory_capture(registration)
                logging.info('Python worker Memray capture cancelled before starting: %s', capture)
                return
            pending = capture.with_suffix('.bin.inprogress')
            # Keep compression and report generation off the worker's shutdown path.
            with Tracker(destination=FileDestination(pending, compress_on_exit=False),
                         native_traces=native == 'true'):
                logging.info('Python worker Memray capture started: pid=%s, started_at=%s, file=%s',
                             os.getpid(), time.time(), pending)
                ready.set()
                stop.wait(duration)
            # Only publish a completed artifact after Tracker has flushed and closed it.
            finish_startup_memory_capture(registration)
            pending.replace(capture)
            logging.info('Python worker Memray capture completed: pid=%s, ended_at=%s, file=%s',
                         os.getpid(), time.time(), capture)
        except Exception:
            # This boundary handles profiler failures, never exceptions from the harness.
            logging.warning('Python worker Memray capture failed: %s', capture, exc_info=True)
        finally:
            ready.set()

    logging.info('Python worker Memray capture enabled: pid=%s, delay=%ss, duration=%ss, '
                 'native_traces=%s, file=%s', os.getpid(), delay, duration, native, capture)
    recorder = Thread(target=record, name='pyflink-memray', daemon=True)
    try:
        recorder.start()
    except RuntimeError:
        finish_startup_memory_capture(registration)
        logging.warning('Could not start Python worker Memray capture', exc_info=True)
        yield
        return

    try:
        if delay == 0:
            # Start before the harness imports user code when no delay is requested.
            ready.wait()
        yield
    finally:
        stop.set()
        recorder.join()
