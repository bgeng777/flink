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
"""Continuous standalone job with identifiable Python and zlib allocation stacks."""

from collections import deque
import os
import zlib

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import ScalarFunction, udf


def allocate_image_buffer(value):
    image = bytearray(640 * 480 * 3)
    image[0] = value % 256
    return image


def compress_image_buffer(image):
    return zlib.compress(image)


class ImageBuffers(ScalarFunction):
    def open(self, function_context):
        self.retained_images = deque(maxlen=16)
        self.count = 0

    def eval(self, value):
        image = allocate_image_buffer(value)
        self.retained_images.append(image)
        compressed = compress_image_buffer(image)
        self.count += 1
        return 'worker=%s,sequence=%s,compressed=%s' % (
            os.getpid(), self.count, len(compressed))


if __name__ == '__main__':
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = env.get_config().get_configuration()
    config.set_string('pipeline.name', 'PyFlink memory profiling standalone smoke test')
    config.set_integer('parallelism.default', 1)
    config.set_string('python.execution-mode', 'process')
    config.set_integer('python.fn-execution.bundle.size', 10)
    config.set_integer('python.fn-execution.bundle.time', 100)
    env.create_temporary_system_function(
        'image_buffers', udf(ImageBuffers(), result_type=DataTypes.STRING()))
    env.execute_sql("""
        CREATE TABLE image_source (sequence_id BIGINT) WITH (
            'connector' = 'datagen', 'rows-per-second' = '40',
            'fields.sequence_id.min' = '1', 'fields.sequence_id.max' = '1000000'
        )
    """)
    env.execute_sql("""
        CREATE TABLE image_results (diagnostic STRING) WITH ('connector' = 'print')
    """)
    result = env.execute_sql('''
        INSERT INTO image_results SELECT image_buffers(sequence_id) FROM image_source
    ''')
    print('MEMORY_SMOKE_JOB_ID=' + str(result.get_job_client().get_job_id()), flush=True)
    result.wait()
