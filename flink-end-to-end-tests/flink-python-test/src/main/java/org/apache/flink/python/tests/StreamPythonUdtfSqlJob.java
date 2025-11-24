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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** A simple job used to test submitting the Python UDF job in stream mode. */
public class StreamPythonUdtfSqlJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration config = tEnv.getConfig().getConfiguration();
        config.setString(
                "python.files",
                "file:///Users/kenken/opensource/flink/flink-end-to-end-tests/flink-python-test/python/add_one.py");
        config.setString("python.executable", "/Users/kenken/opensource/py312/bin/python");
        config.setString("python.client.executable", "/Users/kenken/opensource/py312/bin/python");
        tEnv.executeSql(
                "create temporary system function add_one as 'add_one.add_one' language python");
        tEnv.executeSql(
                "create temporary system function add_one_udtf as 'add_one.add_one_udtf' language python");

        tEnv.createTemporaryView("source", tEnv.fromValues(1L, 2L, 3L).as("a"));

        //        Iterator<Row> result = tEnv.executeSql("select add_one(a) as a from
        // source").collect();
        Iterator<Row> result =
                tEnv.executeSql(
                                "SELECT a, word "
                                        + "FROM source, LATERAL TABLE(add_one_udtf(a)) AS T(word)")
                        .collect();
        List<Row> actual = new ArrayList<>();
        while (result.hasNext()) {
            Row r = result.next();
            actual.add(r);
        }

        System.out.println(actual);
        //        List<Long> expected = Arrays.asList(2L, 3L, 4L);
        //        if (!actual.equals(expected)) {
        //            throw new AssertionError(
        //                    String.format(
        //                            "The output result: %s is not as expected: %s!", actual,
        // expected));
        //        }
    }
}
