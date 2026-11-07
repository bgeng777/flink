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
"""Real process-mode DataStream workload for the local profiling experiment."""
import json
import os
import sys
import time

from pyflink.common import Configuration, Types
from pyflink.datastream import StreamExecutionEnvironment


def cpu_work(value):
    deadline = time.monotonic() + 3
    checksum = 0
    while time.monotonic() < deadline:
        checksum = sum(x * x for x in range(500))
    return os.getpid(), value, checksum


if __name__ == '__main__':
    configuration = Configuration()
    configuration.set_string('python.execution-mode', 'process')
    configuration.set_string('python.executable', sys.executable)
    configuration.set_string('restart-strategy.type', 'none')
    configuration.set_boolean('rest.profiling.enabled', True)
    configuration.set_string('rest.bind-address', '127.0.0.1')
    configuration.set_string('rest.bind-port', os.environ['PYFLINK_PROFILE_REST_PORT'])
    configuration.set_string('rest.python-profiling.cpu.executable',
                             os.environ['PYFLINK_PROFILE_EXECUTABLE'])
    configuration.set_string('rest.profiling.dir', os.environ['PYFLINK_PROFILE_OUTPUT'])
    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_parallelism(1)
    output = env.from_collection([1, 2, 3, 4], Types.INT()).map(
        cpu_work, output_type=Types.TUPLE([Types.LONG(), Types.INT(), Types.LONG()]))
    rows = list(output.execute_and_collect())
    print(json.dumps({'client_pid': os.getpid(), 'rows': rows}), flush=True)
