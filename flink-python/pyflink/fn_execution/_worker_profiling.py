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
"""Experimental local worker registration; not a supported configuration/API."""

import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import uuid
from contextlib import contextmanager


def process_identity(pid):
    """Return an OS start token, or None after the process exits.

    The macOS ps token has second precision and is only for this local prototype.
    The TaskManager also verifies the worker is its descendant before sampling.
    """
    if sys.platform.startswith('linux'):
        try:
            stat = Path('/proc/%d/stat' % pid).read_text()
            boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        except FileNotFoundError:
            return None
        fields = stat.rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else '%s:%s' % (boot, fields[19])
    if sys.platform == 'darwin':
        result = subprocess.run(
            ['ps', '-p', str(pid), '-o', 'lstart=', '-o', 'stat='],
            capture_output=True, text=True, timeout=2,
            env={**os.environ, 'LC_ALL': 'C'})
        fields = result.stdout.split()
        if result.returncode or not fields or fields[-1].startswith('Z'):
            return None
        return ' '.join(fields[:-1])
    raise OSError('The local profiling prototype supports Linux and macOS only')


@contextmanager
def register_worker():
    directory = os.environ.get('PYFLINK_WORKER_PROFILE_DIR')
    if not directory:
        yield
        return

    path = None
    try:
        root = Path(directory)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        registration = {
            'registration_id': uuid.uuid4().hex,
            'worker_id': os.environ['WORKER_ID'],
            'pid': os.getpid(),
            'start_token': process_identity(os.getpid()),
        }
        path = root / (registration['registration_id'] + '.json')
        temporary = path.with_suffix('.tmp')
        with temporary.open('x') as output:
            os.chmod(temporary, 0o600)
            json.dump(registration, output)
        temporary.replace(path)
        logging.info('Registered Python worker for local profiling: %s', registration)
    except (OSError, KeyError, subprocess.SubprocessError):
        # An optional diagnostic facility must not prevent the SDK harness starting.
        logging.warning('Could not register Python worker for profiling', exc_info=True)

    try:
        yield
    finally:
        if path is not None:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logging.warning('Could not remove Python profiling registration', exc_info=True)
