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
"""Local-only experimental REST controller; not a Flink REST endpoint."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
import subprocess
import threading
import time
import uuid

from pyflink.fn_execution._worker_profiling import process_identity


class BusyError(Exception):
    pass


class ProfilingService:
    def __init__(self, registry, output, executable):
        self.registry = Path(registry)
        self.output = Path(output)
        self.output.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.executable = executable
        self._profiles = {}
        self._cancel = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1)

    def workers(self):
        workers = []
        for path in self.registry.glob('*.json'):
            try:
                worker = json.loads(path.read_text())
            except FileNotFoundError:
                continue  # Normal worker shutdown can race with directory enumeration.
            if process_identity(worker['pid']) == worker['start_token']:
                workers.append(worker)
        return workers

    def start(self, worker_id, duration=10, rate=50, include_idle=False):
        if type(duration) is not int or not 1 <= duration <= 60:
            raise ValueError('duration_seconds must be an integer between 1 and 60')
        if type(rate) is not int or not 1 <= rate <= 100:
            raise ValueError('rate_hz must be an integer between 1 and 100')
        if type(include_idle) is not bool:
            raise ValueError('include_idle must be a boolean')
        worker = next((w for w in self.workers() if w['registration_id'] == worker_id), None)
        if worker is None:
            raise KeyError('Worker is absent or has exited')
        with self._lock:
            if any(p['status'] == 'RUNNING' for p in self._profiles.values()):
                raise BusyError('One profiling session is already running')
            while len(self._profiles) >= 20:
                old = next(iter(self._profiles))
                for suffix in ('.svg', '.log'):
                    (self.output / (old + suffix)).unlink(missing_ok=True)
                del self._profiles[old]
                del self._cancel[old]
            profile_id = uuid.uuid4().hex
            profile = {
                'profile_id': profile_id, 'worker': worker, 'status': 'RUNNING',
                'duration_seconds': duration, 'rate_hz': rate, 'include_idle': include_idle,
                'started_at': time.time(), 'error_code': None, 'error': None,
            }
            self._profiles[profile_id] = profile
            self._cancel[profile_id] = threading.Event()
            self._executor.submit(self._record, profile_id)
            return dict(profile)

    def get(self, profile_id):
        with self._lock:
            return dict(self._profiles[profile_id])

    def cancel(self, profile_id):
        with self._lock:
            self._cancel[profile_id].set()

    def result(self, profile_id):
        if self.get(profile_id)['status'] != 'SUCCESS':
            raise BusyError('This session has no completed result')
        return (self.output / (profile_id + '.svg')).read_bytes()

    def _record(self, profile_id):
        profile = self.get(profile_id)
        worker = profile['worker']
        cancel = self._cancel[profile_id]
        output = self.output / (profile_id + '.svg')
        log = self.output / (profile_id + '.log')
        command = [self.executable, 'record', '--pid', str(worker['pid']),
                   '--duration', str(profile['duration_seconds']),
                   '--rate', str(profile['rate_hz']), '--format', 'flamegraph',
                   '--threads', '--output', str(output)]
        if profile['include_idle']:
            command.append('--idle')
        process = None
        status, code, error = 'FAILED', None, None
        try:
            if process_identity(worker['pid']) != worker['start_token']:
                code, error = 'WORKER_EXITED', 'Worker identity changed before sampling'
            elif cancel.is_set():
                status = 'CANCELLED'
            else:
                with log.open('wb') as diagnostic:
                    process = subprocess.Popen(command, stdout=diagnostic,
                                               stderr=subprocess.STDOUT)
                    deadline = time.monotonic() + profile['duration_seconds'] + 5
                    while process.poll() is None:
                        if cancel.wait(0.1):
                            status = 'CANCELLED'
                            break
                        if process_identity(worker['pid']) != worker['start_token']:
                            code, error = 'WORKER_EXITED', 'Worker exited during sampling'
                            break
                        if time.monotonic() >= deadline:
                            code, error = 'TIMEOUT', 'Sampler exceeded its deadline'
                            break
                        if log.stat().st_size > 1024 * 1024 or (
                                output.exists() and output.stat().st_size > 20 * 1024 * 1024):
                            code, error = 'OUTPUT_LIMIT', 'Sampler output exceeded the local limit'
                            break
                if status != 'CANCELLED' and code is None:
                    if process.returncode != 0:
                        with log.open('rb') as diagnostic:
                            error = diagnostic.read(4096).decode('utf-8', errors='replace')
                        permission_error = any(
                            s in error.lower() for s in
                            ('permission', 'requires root', 'operation not permitted'))
                        code = 'PERMISSION_DENIED' if permission_error else 'SAMPLER_FAILED'
                    elif process_identity(worker['pid']) != worker['start_token']:
                        code = 'WORKER_EXITED'
                        error = 'Worker exited before capture completed'
                    elif not output.exists() or output.stat().st_size == 0:
                        code, error = 'NO_OUTPUT', 'Sampler produced no flamegraph'
                    elif output.stat().st_size > 20 * 1024 * 1024:
                        code, error = 'OUTPUT_LIMIT', 'Sampler output exceeded the local limit'
                    else:
                        status = 'SUCCESS'
        except FileNotFoundError as exc:
            code, error = 'SAMPLER_NOT_FOUND', str(exc)
        except (OSError, subprocess.SubprocessError) as exc:
            code, error = 'SAMPLER_FAILED', str(exc)
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if status != 'SUCCESS':
                try:
                    output.unlink(missing_ok=True)
                except OSError:
                    logging.warning(
                        'Could not remove partial profile %s', profile_id, exc_info=True)
            with self._lock:
                self._profiles[profile_id].update(
                    status=status, error_code=code, error=error, finished_at=time.time())

    def close(self):
        with self._lock:
            for cancel in self._cancel.values():
                cancel.set()
        self._executor.shutdown(wait=True)


def make_server(service, port=0):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def respond(self, status, data):
            body = json.dumps(data).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def dispatch(self):
            parts = self.path.strip('/').split('/')
            try:
                if self.command == 'GET' and parts == ['workers']:
                    return self.respond(200, service.workers())
                if (self.command == 'POST' and len(parts) == 3 and
                        parts[0] == 'workers' and parts[2] == 'profiles'):
                    if self.headers.get_content_type() != 'application/json':
                        raise ValueError('Content-Type must be application/json')
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= 4096:
                        raise ValueError('JSON body must be between 1 and 4096 bytes')
                    data = json.loads(self.rfile.read(length))
                    if not isinstance(data, dict) or set(data) - {
                            'duration_seconds', 'rate_hz', 'include_idle'}:
                        raise ValueError('Unsupported profiling parameters')
                    return self.respond(202, service.start(
                        parts[1], data.get('duration_seconds', 10),
                        data.get('rate_hz', 50), data.get('include_idle', False)))
                if len(parts) >= 2 and parts[0] == 'profiles':
                    if self.command == 'GET' and len(parts) == 2:
                        return self.respond(200, service.get(parts[1]))
                    if self.command == 'DELETE' and len(parts) == 2:
                        service.cancel(parts[1])
                        return self.respond(202, service.get(parts[1]))
                    if self.command == 'GET' and len(parts) == 3 and parts[2] == 'result':
                        body = service.result(parts[1])
                        self.send_response(200)
                        self.send_header('Content-Type', 'image/svg+xml')
                        self.send_header('Content-Disposition', 'attachment; filename=profile.svg')
                        self.send_header('Content-Security-Policy', "sandbox; default-src 'none'")
                        self.send_header('X-Content-Type-Options', 'nosniff')
                        self.send_header('Content-Length', str(len(body)))
                        self.end_headers()
                        return self.wfile.write(body)
                self.respond(404, {'error': 'Unknown endpoint'})
            except KeyError as exc:
                self.respond(404, {'error': str(exc)})
            except BusyError as exc:
                self.respond(409, {'error': str(exc)})
            except (ValueError, UnicodeError) as exc:
                self.respond(400, {'error': str(exc)})
            except OSError as exc:
                self.respond(500, {'error': str(exc)})

        do_GET = dispatch
        do_POST = dispatch
        do_DELETE = dispatch

    return ThreadingHTTPServer(('127.0.0.1', port), Handler)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--py-spy', default='py-spy')
    parser.add_argument('--port', type=int, default=18081)
    args = parser.parse_args()
    service = ProfilingService(args.registry, args.output, args.py_spy)
    try:
        with make_server(service, args.port) as server:
            print('Local profiling prototype: http://127.0.0.1:%d' % server.server_port, flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    finally:
        service.close()
