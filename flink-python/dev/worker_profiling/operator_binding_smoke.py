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
"""Two explicitly unfused Python operators for worker-selection validation."""

import argparse
from collections import deque
import json
import os
from pathlib import Path
import time

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.datastream.functions import MapFunction


def decode_hotspot(value):
    result = value
    for index in range(45000):
        result = (result * 31 + index) % 1000000007
    return result


def infer_hotspot(value):
    result = value
    for index in range(65000):
        result = (result * 17 + index) % 1000000007
    return result


def allocate_feature_buffer(value):
    buffer = bytearray(256 * 1024)
    buffer[0] = value % 256
    return buffer


class DecodeImages(MapFunction):
    def __init__(self, directory):
        self.directory = directory

    def open(self, runtime_context):
        write_identity(self.directory, 'decode-images', runtime_context)

    def map(self, value):
        result = decode_hotspot(value)
        time.sleep(0.005)
        return result


class InferImages(MapFunction):
    def __init__(self, directory):
        self.directory = directory

    def open(self, runtime_context):
        write_identity(self.directory, 'infer-images', runtime_context)
        self.features = deque(maxlen=8)
        self.count = 0

    def map(self, value):
        result = infer_hotspot(value)
        self.features.append(allocate_feature_buffer(result))
        self.count += 1
        return 'worker=%s count=%s result=%s' % (os.getpid(), self.count, result)


def write_identity(directory, name, context):
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    record = {'label': name, 'pid': os.getpid(), 'task_name': context.get_task_name(),
              'subtask': context.get_index_of_this_subtask(),
              'attempt': context.get_attempt_number()}
    (path / (name + '.json')).write_text(json.dumps(record))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', required=True)
    parser.add_argument('--managed', action='store_true')
    args = parser.parse_args()
    config = Configuration()
    config.set_boolean('python.fn-execution.memory.managed', args.managed)
    config.set_boolean('python.operator-chaining.enabled', False)
    config.set_string('python.execution-mode', 'process')
    config.set_integer('python.fn-execution.bundle.size', 1)
    config.set_integer('python.fn-execution.bundle.time', 10)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.disable_operator_chaining()
    source = env.from_source(NumberSequenceSource(0, 1000000000),
                             WatermarkStrategy.no_watermarks(), 'sequence')
    decoded = source.map(DecodeImages(args.directory), output_type=Types.LONG())
    decoded.name('decode-images').disable_chaining()
    inferred = decoded.map(InferImages(args.directory), output_type=Types.STRING())
    inferred.name('infer-images').disable_chaining()
    inferred.print()
    directory = Path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'execution-plan.json').write_text(env.get_execution_plan())
    env.execute('Unfused Python operators; managed=' + str(args.managed))


if __name__ == '__main__':
    main()
