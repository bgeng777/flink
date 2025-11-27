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

/** A simple job used to test submitting the Python UDF job in stream mode. */
public class StreamPythonMLPredictSqlJob {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration config = tEnv.getConfig().getConfiguration();

        //        config.setString("python.files",
        // "file:///Users/kenken/PycharmProjects/inferenceDemo/vllm_udtf.py");
        config.setString(
                "python.files",
//                "file:///Users/kenken/opensource/flink/flink-end-to-end-tests/flink-python-test/python/huggingface_udtf.py");
                "file:///Users/kenken/opensource/flink/flink-end-to-end-tests/flink-python-test/python/vllm_udtf.py");
        config.setString("python.executable", "/Users/kenken/opensource/py312/bin/python");
        config.setString("python.client.executable", "/Users/kenken/opensource/py312/bin/python");

        //        tEnv.createTemporaryView("source", tEnv.fromValues("请用一句话介绍什么是量子计算", "wednesday is
        // good day", "what is vllm").as("question"));
        tEnv.createTemporaryView(
                "source",
                tEnv.fromValues(
                                // 声明 schema
                                DataTypes.ROW(
                                        DataTypes.FIELD("text", DataTypes.STRING()),
                                        DataTypes.FIELD("src_comment", DataTypes.STRING()),
                                        DataTypes.FIELD("src_length", DataTypes.INT())),
                                // 填入几行测试数据
                                row("请用一句话介绍什么是量子计算", "first row", 5),
                                row("wednesday is good day", "second row", 5),
                                row("what is vllm", "third row", 9))
                        .as("text", "src_comment", "src_length"));
        tEnv.executeSql(
                "CREATE MODEL my_python_model\n"
                        + "INPUT (text STRING, i_comment STRING)\n"
                        + "OUTPUT (prediction STRING, "
                        + " length INT)\n"
                        + "WITH (\n"
                        + "   'provider' = 'generic-python',\n"
                        + "   'model' = '/Users/kenken/.cache/modelscope/hub/models/Qwen/Qwen3-0.6B',\n"
//                        + "   'python-predict-function' = 'huggingface_udtf.HuggingFaceMLUDTF',\n"
                        + "   'python-predict-function' = 'vllm_udtf.VLLMMLUDTF',\n"
                        + "   'properties.device_map' = 'auto'\n"
                        + ")");
        //        System.out.println(
        //                tEnv.explainSql(
        //                        "SELECT text, src_comment, prediction, length "
        //                                + "FROM ML_PREDICT(TABLE source, MODEL my_python_model,
        // DESCRIPTOR(text, src_comment)) "));
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT text, src_comment, prediction, length "
                                                + "FROM ML_PREDICT(TABLE source, MODEL my_python_model, DESCRIPTOR(text, src_comment)) ")
                                .collect());

        for (Row row : result) {
            System.out.println(row);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total time: " + (endTime - startTime));
    }
}
