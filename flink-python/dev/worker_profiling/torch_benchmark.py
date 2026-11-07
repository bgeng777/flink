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
"""Run the local two-TM PyTorch profiling experiment and preserve raw measurements."""

import argparse
import json
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class Experiment:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.directory)
        self.root.mkdir(parents=True, exist_ok=True)
        wrapper = self.root / 'python-single-thread'
        wrapper.write_text(
            '#!/bin/sh\nexport OMP_NUM_THREADS=1 OMP_THREAD_LIMIT=1 OPENBLAS_NUM_THREADS=1 '
            'MKL_NUM_THREADS=1\nexec /usr/local/bin/python3 "$@"\n')
        wrapper.chmod(0o755)
        self.clients = []
        self.jobs = []

    def api(self, path, body=None, method=None):
        request = Request(
            self.args.url + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Content-Type': 'application/json'}, method=method)
        with urlopen(request, timeout=20) as response:
            return json.load(response)

    @staticmethod
    def wait(predicate, timeout=150):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.25)
        raise TimeoutError('Experiment condition did not become true')

    def command(self, *args):
        return subprocess.check_output(
            ['podman', 'exec', self.args.container, *args], text=True)

    def workers(self):
        result = []
        for tm in self.api('/taskmanagers')['taskmanagers']:
            for worker in self.api('/taskmanagers/' + tm['id'] + '/python-workers')['workers']:
                result.append({'tm': tm['id'], **worker})
        return result

    def start_job(self, label, parallelism, threads, rate=100000):
        directory = self.root / label
        directory.mkdir()
        log = (directory / 'client.log').open('w')
        command = [
            'podman', 'exec', self.args.container, '/opt/flink/bin/flink', 'run', '-py',
            '/opt/flink-python/dev/worker_profiling/torch_smoke.py',
            '--directory', '/evidence/torch-multiworker/' + label,
            '--parallelism', str(parallelism), '--threads', str(threads), '--rate', str(rate),
        ]
        client = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        self.clients.append((client, log))
        job = self.wait(lambda: next((j['jid'] for j in self.api('/jobs/overview')['jobs']
                                     if j['state'] == 'RUNNING'
                                     and j['jid'] not in self.jobs), None))
        self.jobs.append(job)
        metadata = self.wait(lambda: list(directory.glob('worker-*.json'))
                             if len(list(directory.glob('worker-*.json'))) == parallelism else None)
        self.wait(lambda: all('eval_intraop_after' in json.loads(p.read_text())
                              for p in metadata))
        workers = self.wait(lambda: self.workers() if len(self.workers()) == parallelism else None)
        print('JOB_READY', label, json.dumps(workers), flush=True)
        return job, directory, workers, [json.loads(p.read_text()) for p in metadata]

    def stop_job(self, job):
        self.api('/jobs/' + job + '?mode=cancel', method='PATCH')
        self.wait(lambda: self.api('/jobs/' + job)['state'] == 'CANCELED')
        self.wait(lambda: not self.workers())

    def start_capture(self, worker, kind, duration, native=False):
        return self.api('/taskmanagers/' + worker['tm'] + '/profiler', {
            'workerId': worker['id'], 'type': kind, 'duration': duration,
            'includeNative': native,
        })

    def finish_capture(self, worker, accepted, label):
        route = '/taskmanagers/' + worker['tm'] + '/profiler'

        def finished():
            items = self.api(route + '?target=python')['profilingList']
            item = next(x for x in items if x['triggerTime'] == accepted['triggerTime'])
            return item if item['status'] != 'RUNNING' else None

        result = self.wait(finished)
        (self.root / (label + '-profile.json')).write_text(json.dumps(result, indent=2))
        assert result['status'] == 'FINISHED', result
        assert result['worker']['id'] == worker['id']
        with urlopen(self.args.url + route + '/' + result['outputFile'], timeout=20) as response:
            downloaded = response.read()
        original = self.root / 'profiles' / result['outputFile']
        assert original.read_bytes() == downloaded
        if result['type'] == 'MEMORY':
            script = (
                'import json,memray,sys; r=memray.FileReader(sys.argv[1]); m=r.metadata; '
                'print(json.dumps(dict(pid=m.pid,start=m.start_time.timestamp(),'
                'end=m.end_time.timestamp(),peak_memory=m.peak_memory,'
                'native=m.has_native_traces,total_allocations=m.total_allocations,'
                'known_stack=any(any(f[0]=="eval" and "torch_smoke" in f[1] '
                'for f in a.stack_trace()) for a in r.get_high_watermark_allocation_records()))))')
            result['memray'] = json.loads(self.command(
                'python', '-c', script,
                '/evidence/torch-multiworker/profiles/' + result['outputFile']))
            assert result['memray']['pid'] == worker['pid'], result
        return result

    def snapshot(self, worker):
        script = '''import json,os,sys,time
from pathlib import Path
pid=int(sys.argv[1]); base=Path('/proc')/str(pid); clock=os.sysconf('SC_CLK_TCK')
def cpu(path):
 f=path.read_text().rsplit(')',1)[1].split();return (int(f[11])+int(f[12]))/clock
threads=[]
for task in (base/'task').iterdir():
 try:
  threads.append(dict(tid=int(task.name),name=(task/'comm').read_text().strip(),
                      cpu=cpu(task/'stat')))
 except FileNotFoundError: pass
memory={}
for line in (base/'smaps_rollup').read_text().splitlines():
 if line.startswith(('Rss:','Pss:')):
  k,v,*_=line.split();memory[k.rstrip(':')]=int(v)*1024
print(json.dumps(dict(time=time.time(),pid=pid,cpu=cpu(base/'stat'),
                      threads=threads,memory=memory)))'''
        return json.loads(self.command('python', '-c', script, str(worker['pid'])))

    def multi(self):
        job, directory, workers, metadata = self.start_job('multi-tm', 2, 1, rate=20)
        assert len({w['tm'] for w in workers}) == 2, workers
        assert {w['pid'] for w in workers} == {m['pid'] for m in metadata}
        try:
            self.api('/taskmanagers/' + workers[0]['tm'] + '/profiler', {
                'workerId': workers[1]['id'], 'type': 'CPU', 'duration': 8})
            raise AssertionError('A TM accepted another TM worker')
        except HTTPError as error:
            assert error.code == 404, error.code
        results = []
        for index in range(2):
            cpu_worker, memory_worker = workers[index], workers[1 - index]
            cpu = self.start_capture(cpu_worker, 'CPU', 8, native=True)
            memory = self.start_capture(memory_worker, 'MEMORY', 8, native=True)
            try:
                self.start_capture(cpu_worker, 'CPU', 8)
                raise AssertionError('Overlapping capture accepted in one TM')
            except HTTPError as error:
                assert error.code == 409, error.code
            results.append(self.finish_capture(cpu_worker, cpu, 'multi-cpu-' + str(index)))
            results.append(self.finish_capture(memory_worker, memory, 'multi-memory-' + str(index)))
            assert self.api('/jobs/' + job)['state'] == 'RUNNING'
        result = {'workers': workers, 'metadata': metadata, 'captures': results,
                  'cross_tm_worker_rejected': True, 'same_tm_overlap_rejected': True,
                  'different_tm_overlap_succeeded': True}
        (directory / 'result.json').write_text(json.dumps(result, indent=2))
        print('MULTI_SUCCESS', flush=True)
        self.stop_job(job)

    def benchmark(self, threads):
        label = 'threads-' + str(threads)
        job, directory, workers, metadata = self.start_job(label, 1, threads)
        worker = workers[0]
        time.sleep(5)
        modes = self.args.modes
        phases = []
        for repeat in range(self.args.repeats):
            order = modes[repeat:] + modes[:repeat]
            for mode in order:
                before = self.snapshot(worker)
                begin = time.time()
                phase = {'repeat': repeat, 'mode': mode, 'before': before, 'begin': begin}
                if mode == 'baseline':
                    time.sleep(self.args.duration)
                    end = time.time()
                    phase['window'] = [begin + 2, end - 2]
                else:
                    kind = 'MEMORY' if mode == 'memory' else 'CPU'
                    accepted = self.start_capture(
                        worker, kind, self.args.duration, native=mode == 'cpu-native')
                    result = self.finish_capture(
                        worker, accepted, '%s-%s-%s' % (label, repeat, mode))
                    phase['profile'] = result
                    if kind == 'MEMORY':
                        phase['window'] = [result['memray']['start'] + 2,
                                           result['memray']['end'] - 2]
                    else:
                        phase['window'] = [result['triggerTime'] / 1000 + 2,
                                           result['finishedTime'] / 1000 - 2]
                phase['end'] = time.time()
                phase['after'] = self.snapshot(worker)
                phases.append(phase)
                (directory / 'phases.json').write_text(json.dumps({
                    'metadata': metadata, 'worker': worker, 'phases': phases,
                }, indent=2))
                print('PHASE_DONE', label, repeat, mode, round(phase['end'] - begin, 2), flush=True)
                assert self.api('/jobs/' + job)['state'] == 'RUNNING'
                time.sleep(3)
        self.stop_job(job)

    def close(self):
        for job in self.jobs:
            try:
                if self.api('/jobs/' + job)['state'] not in ('CANCELED', 'FINISHED', 'FAILED'):
                    self.stop_job(job)
            except Exception as error:
                print('Job cleanup failed:', job, str(error), flush=True)
        for client, log in self.clients:
            try:
                client.wait(timeout=20)
            except subprocess.TimeoutExpired:
                client.terminate()
            log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', required=True)
    parser.add_argument('--url', default='http://127.0.0.1:18081')
    parser.add_argument('--container', default='pyflink-flip-e2e-20261107')
    parser.add_argument('--stage', choices=['multi', 'bench', 'all'], default='all')
    parser.add_argument('--duration', type=int, default=20)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--threads', type=int, nargs='+', choices=[0, 1], default=[1, 0])
    parser.add_argument('--modes', nargs='+',
                        choices=['baseline', 'cpu', 'memory', 'cpu-native'],
                        default=['baseline', 'cpu', 'memory', 'cpu-native'])
    args = parser.parse_args()
    experiment = Experiment(args)
    try:
        experiment.wait(lambda: len(experiment.api('/taskmanagers')['taskmanagers']) == 2)
        if args.stage in ('multi', 'all'):
            experiment.multi()
        if args.stage in ('bench', 'all'):
            for threads in args.threads:
                experiment.benchmark(threads)
    finally:
        experiment.close()


if __name__ == '__main__':
    main()
