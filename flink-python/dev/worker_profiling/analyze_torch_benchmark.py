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
"""Summarize raw profiling intervals without treating a local experiment as a guarantee."""

import argparse
import json
from pathlib import Path
import re
import statistics
import xml.etree.ElementTree as ET


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    summary = {'measurements': [], 'summary': [], 'native_visibility': []}
    for directory in sorted(p for p in args.directory.glob('threads-*') if p.is_dir()):
        data = json.loads((directory / 'phases.json').read_text())
        intervals = []
        for file in directory.glob('worker-*.jsonl'):
            intervals.extend(json.loads(line) for line in file.read_text().splitlines())
        for phase in data['phases']:
            lo, hi = phase['window']
            complete = [row for row in intervals if row['start'] >= lo and row['end'] <= hi]
            latencies = [value for row in complete for value in row['latency_ms']]
            assert latencies, (directory, phase['mode'], phase['window'])
            seconds = sum(row['end'] - row['start'] for row in complete)
            measurement = {
                'threads': data['metadata'][0]['intraop_threads'],
                'policy': directory.name, 'repeat': phase['repeat'], 'mode': phase['mode'],
                'records': len(latencies), 'seconds': seconds,
                'records_per_second': len(latencies) / seconds,
                'p50_ms': percentile(latencies, 0.5), 'p95_ms': percentile(latencies, 0.95),
                'p99_ms': percentile(latencies, 0.99),
                'worker_cpu_cores': (phase['after']['cpu'] - phase['before']['cpu'])
                / (phase['after']['time'] - phase['before']['time']),
                'pss_before': phase['before']['memory']['Pss'],
                'pss_after': phase['after']['memory']['Pss'],
            }
            summary['measurements'].append(measurement)
            if phase['mode'] == 'cpu-native':
                file = args.directory / 'profiles' / phase['profile']['outputFile']
                titles = [element.text or '' for element in ET.parse(file).iter()
                          if element.tag.endswith('title')]
                visible = set()
                for title in titles:
                    match = re.search(r'thread \((\d+)\)', title, re.IGNORECASE)
                    if match:
                        visible.add(int(match.group(1)))
                before = {t['tid']: t['cpu'] for t in phase['before']['threads']}
                active = []
                for thread in phase['after']['threads']:
                    delta = thread['cpu'] - before.get(thread['tid'], thread['cpu'])
                    if delta > 0.1:
                        active.append({**thread, 'cpu_delta': delta,
                                       'visible_in_svg': thread['tid'] in visible})
                summary['native_visibility'].append({
                    'policy': directory.name, 'repeat': phase['repeat'],
                    'visible_thread_ids': sorted(visible), 'active_threads': active,
                    'torch_frames': [title for title in titles
                                     if any(token in title for token in (
                                         'conv2d', '_conv_forward', 'libtorch', 'at::native',
                                         'torch_smoke.py', 'resnet.py'))][:30],
                })
        rows = [r for r in summary['measurements'] if r['policy'] == directory.name]
        baseline = statistics.median(r['records_per_second'] for r in rows
                                     if r['mode'] == 'baseline')
        for mode in ('baseline', 'cpu', 'cpu-native', 'memory'):
            runs = [r for r in rows if r['mode'] == mode]
            if not runs:
                continue
            rate = statistics.median(r['records_per_second'] for r in runs)
            summary['summary'].append({
                'policy': directory.name, 'threads': runs[0]['threads'], 'mode': mode,
                'repeats': len(runs), 'median_records_per_second': rate,
                'rate_range': [min(r['records_per_second'] for r in runs),
                               max(r['records_per_second'] for r in runs)],
                'throughput_change_percent': (rate / baseline - 1) * 100,
                'median_p50_ms': statistics.median(r['p50_ms'] for r in runs),
                'median_p95_ms': statistics.median(r['p95_ms'] for r in runs),
                'median_p99_ms': statistics.median(r['p99_ms'] for r in runs),
            })
    (args.directory / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary['summary'], indent=2))


if __name__ == '__main__':
    main()
