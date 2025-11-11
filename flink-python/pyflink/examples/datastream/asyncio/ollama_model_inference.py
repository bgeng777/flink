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
import argparse
import logging
import sys
from typing import List
from ollama import AsyncClient

from pyflink.common import Encoder, Types, Time, Row
from pyflink.datastream import (
    StreamExecutionEnvironment,
    AsyncDataStream,
    AsyncFunction,
    RuntimeContext,
    CheckpointingMode,
)
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)


class AsyncLLMRequest(AsyncFunction[Row, str]):

    def __init__(self):
        self.retried_keys = {}

    def open(self, runtime_context: RuntimeContext):
        # create model inference client here
        pass

    def close(self):
        # close the model inference client here
        pass

    async def async_invoke(self, value: Row) -> List[str]:
        message = {"role": "user", "content": value.question}
        question_id = value.id
        ollam_response = await AsyncClient().chat(model="qwen3:4b", messages=[message])
        return [
            f"Question ID {question_id}: response: {ollam_response['message']['content']}"
        ]

    def timeout(self, value: Row) -> List[str]:
        # return a default value in case timeout
        return [f"Timeout for this question: {value.a}"]


def main(output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)
    ds = env.from_collection(
        [
            ("Who are you?", 0),
            ("Tell me a joke", 1),
            ("Tell me the result of comparing 0.8 and 0.11", 2),
        ],
        type_info=Types.ROW_NAMED(["question", "id"], [Types.STRING(), Types.INT()]),
    )

    # Async version:
    result_stream = AsyncDataStream.unordered_wait(
        data_stream=ds,
        async_function=AsyncLLMRequest(),
        timeout=Time.seconds(100),
        # async_retry_strategy=async_retry_strategy,
        capacity=1000,
        output_type=Types.STRING(),
    )

    # Sync version:
    # class MyMapFunction(MapFunction):
    #     def map(self, value):
    #         message = {'role': 'user', 'content': value.question}
    #         question_id = value.id
    #         ollam_response = Client().chat(model="qwen3:4b", messages=[message])
    #         return f"Question ID {question_id}: response: {ollam_response}"
    # result_stream = ds.map(func=MyMapFunction(),  output_type=Types.STRING())

    # define the sink
    if output_path is not None:
        result_stream.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path, encoder=Encoder.simple_string_encoder()
            )
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
            )
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        result_stream.print()

    # submit for execution
    env.execute()


if __name__ == "__main__":
    import time

    start_time = time.time()
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        dest="output",
        required=False,
        help="Output file to write results to.",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(known_args.output)
    end_time = time.time()  # 记录结束时间（秒）
    elapsed = end_time - start_time  # 计算耗时
    print(f"Running time: {elapsed:.3f} seconds")
