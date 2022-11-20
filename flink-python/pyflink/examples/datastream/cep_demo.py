################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################


# 1. pattern in Python
# 2. call Java CEPOperator to create NFA and process element
# 3. call Python udf to process matches

import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.cep import cep
from pyflink.datastream.cep.condition import Condition
from pyflink.datastream.cep.pattern import Pattern
from pyflink.datastream.connectors.file_system import (FileSource, StreamFormat, FileSink,
                                                       OutputFileConfig, RollingPolicy)

cep_demo_data = [
    "1,1,1",
    "2,1,1",
    "3,1,1"
]


def create_demo_pattern():
    pattern = Pattern("start")
    class DemoCondition(Condition):

        def filter(self, value):
            return True
    pattern.where(DemoCondition())
    return pattern


def cep_demo(input_path, output_path):
    config = Configuration()
    config.set_string("python.execution-mode", "thread")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    # define the sourc        ds = env.from_source(

    ds = env.from_collection(cep_demo_data)

    def split(line):
        yield from line.split()

    pattern = create_demo_pattern()

    # output = cep.pattern(input, pattern)
    def filter_func(line):
        return line.split(",")[1] == "1"

    # output = ds.filter(
    #     filter_func
    # ).print()
    output = cep.pattern(ds, pattern)
    # compute word count
    # ds = ds.flat_map(split) \
    #        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    #        .key_by(lambda i: i[0]) \
    #        .reduce(lambda i, j: (i[0], i[1] + j[1]))
    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    cep_demo(known_args.input, known_args.output)
