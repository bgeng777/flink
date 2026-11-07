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
"""CPU ResNet-18 workload with per-worker interval measurements for profiling experiments."""

import argparse
import json
import os
from pathlib import Path
import time
import threading

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import ScalarFunction, udf


class ImageInference(ScalarFunction):
    def __init__(self, directory, threads):
        self.directory = directory
        self.threads = threads

    def open(self, function_context):
        import numpy as np
        import torch
        import torchvision
        from torchvision.models import resnet18
        from threadpoolctl import threadpool_info

        self.threadpool_info = threadpool_info
        self.np = np
        self.torch = torch
        if self.threads:
            torch.set_num_threads(self.threads)
        torch.manual_seed(0)
        self.model = resnet18(weights=None).eval()
        warmup = torch.rand(1, 3, 224, 224)
        with torch.inference_mode():
            for _ in range(20):
                self.model(warmup)
        self.pid = os.getpid()
        directory = Path(self.directory)
        directory.mkdir(parents=True, exist_ok=True)
        metadata = {
            'pid': self.pid,
            'parent_pid': os.getppid(),
            'subtask': function_context.get_index_of_this_subtask(),
            'torch': torch.__version__,
            'torchvision': torchvision.__version__,
            'intraop_threads': torch.get_num_threads(),
            'interop_threads': torch.get_num_interop_threads(),
            'model': 'resnet18(weights=None).eval()',
            'input_shape': [1, 3, 224, 224],
            'input_bytes': 224 * 224 * 3,
            'warmup_iterations': 20,
            'threadpools': threadpool_info(),
            'thread_environment': {k: os.environ.get(k) for k in (
                'OMP_NUM_THREADS', 'OMP_THREAD_LIMIT', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS')},
        }
        self.metadata = metadata
        self.metadata_path = directory / ('worker-%s.json' % self.pid)
        self.eval_thread_recorded = False
        temporary = directory / ('worker-%s.tmp' % self.pid)
        temporary.write_text(json.dumps(metadata))
        temporary.replace(directory / ('worker-%s.json' % self.pid))
        self.measurements = (directory / ('worker-%s.jsonl' % self.pid)).open('a')
        self.interval_start = time.time()
        self.latencies = []
        self.completed = 0

    def eval(self, image):
        # Record the effective settings on the thread executing inference.
        before = self.torch.get_num_threads()
        if self.threads and before != self.threads:
            self.torch.set_num_threads(self.threads)
        if not self.eval_thread_recorded:
            self.metadata['eval_intraop_before'] = before
            self.metadata['eval_intraop_after'] = self.torch.get_num_threads()
            self.metadata['eval_threadpools'] = self.threadpool_info()
            self.metadata['eval_native_thread'] = threading.get_native_id()
            temporary = self.metadata_path.with_suffix('.tmp')
            temporary.write_text(json.dumps(self.metadata))
            temporary.replace(self.metadata_path)
            self.eval_thread_recorded = True
        begin = time.perf_counter_ns()
        # The source sends an image-sized string through the ordinary Python operator path.
        pixels = self.np.frombuffer(image.encode('ascii'), dtype=self.np.uint8).copy()
        tensor = self.torch.from_numpy(pixels.reshape(224, 224, 3)).permute(2, 0, 1)
        tensor = tensor.unsqueeze(0).float().div_(255)
        with self.torch.inference_mode():
            prediction = self.model(tensor)
        result = float(prediction.sum().item())
        self.latencies.append((time.perf_counter_ns() - begin) / 1e6)
        self.completed += 1
        now = time.time()
        if now - self.interval_start >= 1:
            self.flush_interval(now)
        return result

    def flush_interval(self, now):
        self.measurements.write(json.dumps({
            'pid': self.pid, 'start': self.interval_start, 'end': now,
            'completed': self.completed, 'latency_ms': self.latencies,
        }) + '\n')
        self.measurements.flush()
        self.interval_start = now
        self.latencies = []

    def close(self):
        if hasattr(self, 'measurements'):
            self.flush_interval(time.time())
            self.measurements.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', required=True)
    parser.add_argument('--parallelism', type=int, default=1)
    parser.add_argument('--threads', type=int, default=0)
    parser.add_argument('--rate', type=int, default=100000)
    args = parser.parse_args()
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = env.get_config().get_configuration()
    config.set_string('pipeline.name', 'PyFlink CPU ResNet-18 profiling experiment')
    config.set_integer('parallelism.default', args.parallelism)
    config.set_string('python.execution-mode', 'process')
    if args.threads == 1:
        config.set_string('python.executable',
                          str(Path(args.directory).parent / 'python-single-thread'))
    config.set_integer('python.fn-execution.bundle.size', 1)
    config.set_integer('python.fn-execution.bundle.time', 10)
    env.create_temporary_system_function(
        'infer_image', udf(ImageInference(args.directory, args.threads),
                           result_type=DataTypes.DOUBLE()))
    env.execute_sql("""
        CREATE TABLE images (image STRING) WITH (
            'connector' = 'datagen', 'rows-per-second' = '%d',
            'fields.image.length' = '150528'
        )
    """ % args.rate)
    env.execute_sql("""
        CREATE TABLE predictions (score DOUBLE) WITH ('connector' = 'blackhole')
    """)
    result = env.execute_sql('INSERT INTO predictions SELECT infer_image(image) FROM images')
    print('TORCH_JOB_ID=' + str(result.get_job_client().get_job_id()), flush=True)
    result.wait()


if __name__ == '__main__':
    main()
