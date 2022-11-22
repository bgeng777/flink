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

import logging
import sys

from pyflink.common import Types, Configuration, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.cep import cep
from pyflink.datastream.cep.condition import Condition
from pyflink.datastream.cep.pattern import Pattern

cep_demo_data = [
    Row(f0="11", f1=4),
    Row(f0="11", f1=5),
    Row(f0="11", f1=6),
    Row(f0="11", f1=7),
    Row(f0="11", f1=1),
    Row(f0="11", f1=2),
]


def create_demo_pattern():


    class StartCondition(Condition):

        def filter(self, value) -> Row:
            if value.f1 > 3 and value.f1 < 8:
                return Row(f0=1)
            else:
                return Row(f0=0)

    class EndCondition(Condition):

        def filter(self, value) -> Row:
            if value.f1 < 3:
                return Row(f0=1)
            else:
                return Row(f0=0)

    start_pattern = Pattern("start")
    end_pattern = Pattern("end")
    pattern = start_pattern \
        .where(StartCondition()) \
        .times(4) \
        .followedBy(end_pattern) \
        .where(EndCondition()) \
        .times(2)
    return pattern


def cep_demo():
    # use thread mode
    config = Configuration()
    config.set_string("python.execution-mode", "thread")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    env.set_parallelism(1)

    # define the source
    ds = env.from_collection(collection=cep_demo_data,
                             type_info=Types.ROW([Types.STRING(), Types.INT()]))

    pattern = create_demo_pattern()

    output = cep.pattern(ds, pattern).print()
    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    cep_demo()
