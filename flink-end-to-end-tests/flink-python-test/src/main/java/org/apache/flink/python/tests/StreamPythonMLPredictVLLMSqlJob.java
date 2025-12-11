/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.List;

import static org.apache.flink.table.api.Expressions.row;

/** A simple job used to test submitting the Python ML Predict job using vLLM. */
public class StreamPythonMLPredictVLLMSqlJob {
// single: 93941
//    batch: 65039
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration config = tEnv.getConfig().getConfiguration();
        String pythonInterpreterPath = "/Users/kenken/opensource/py312/bin/python";
        String pythonFilesPath =
                "/Users/kenken/opensource/flink/flink-end-to-end-tests/flink-python-test/python/vllm_args_udtf.py";
        config.setString("python.files", "file://" + pythonFilesPath);
        config.setString("python.executable", pythonInterpreterPath);
        config.setString("python.client.executable", pythonInterpreterPath);

        tEnv.createTemporaryView(
                "source",
                tEnv.fromValues(
                                DataTypes.ROW(
                                        DataTypes.FIELD("prompt", DataTypes.STRING()),
                                        DataTypes.FIELD("prompt_comment", DataTypes.STRING()),
                                        DataTypes.FIELD("request_id", DataTypes.INT())),
                                row("tell me a joke", "first row", 5),
                                row("what is pyflink", "second row", 5),
                                row("what is vllm", "third row", 9),
                                row("tell me a joke", "first row", 5),
                                row("what is pyflink", "second row", 5),
                                row("what is vllm", "third row", 9),
                                row("tell me a joke", "first row", 5),
                                row("what is pyflink", "second row", 5),
                                row("what is vllm", "third row", 9),
                                row("what is vllm", "third row", 9))
                        .as("prompt", "prompt_comment", "request_id"));
        tEnv.executeSql(
                "CREATE MODEL my_python_model\n"
                        + "INPUT (prompt STRING, i_comment STRING)\n"
                        + "OUTPUT (prediction STRING, "
                        + " length INT)\n"
                        + "WITH (\n"
                        + "   'provider' = 'generic-python',\n"
                        + "   'model' = '/Users/kenken/.cache/modelscope/hub/models/Qwen/Qwen3-0.6B',\n"
                        + "   'python-predict-class' = 'vllm_args_udtf.VLLMFunc',\n"
                        + "   'properties.vllm.engine_args' = '--max-model-len 256 --trust-remote-code True --dtype half --enforce-eager True',\n"
                        + "   'properties.vllm.sampling_params' = '{\"temperature\" : 0.7, \"top_p\" : 0.9, \"max_tokens\" : 256 }'\n"
                        + ")");
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT prompt, prompt_comment, prediction, length "
                                                + "FROM ML_PREDICT(TABLE source, MODEL my_python_model, DESCRIPTOR(prompt, prompt_comment)) ")
                                .collect());

        for (Row row : result) {
            System.out.println(row);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total time: " + (endTime - startTime));
    }
}
