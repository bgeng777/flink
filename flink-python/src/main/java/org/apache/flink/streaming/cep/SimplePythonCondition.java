/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.cep;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.types.Row;

import pemja.core.PythonInterpreter;
import pemja.core.object.PyIterator;

/**
 * A user-defined condition that decides if an element should be accepted in the pattern or not.
 * Accepting an element also signals a state transition for the corresponding {@link
 * org.apache.flink.cep.nfa.NFA}.
 *
 * <p>Contrary to the {@link IterativeCondition}, conditions that extend this class do not have
 * access to the previously accepted elements in the pattern. Conditions that extend this class are
 * simple {@code filter(...)} functions that decide based on the properties of the element at hand.
 */
@Internal
public class SimplePythonCondition<T> extends SimpleCondition<T> {

    private static final long serialVersionUID = 4942618239408140245L;
    private final String patternName;
    private final PythonInterpreter interpreter;
    private final PythonTypeUtils.DataConverter<T, Object> inputDataConverter;
    private final PythonTypeUtils.DataConverter<?, Object> outputDataConverter;

    public SimplePythonCondition(
            String patternName,
            PythonInterpreter interpreter,
            PythonTypeUtils.DataConverter<T, Object> inputDataConverter,
            PythonTypeUtils.DataConverter<?, Object> outputDataConverter) {
        this.patternName = patternName;
        this.interpreter = interpreter;
        this.inputDataConverter = inputDataConverter;
        this.outputDataConverter = outputDataConverter;
    }

    @Override
    public boolean filter(T value) throws Exception {
        PyIterator results = null;
        if (!patternName.equals("start")) {
            results =
                    (PyIterator)
                            interpreter.invokeMethod(
                                    "operation",
                                    "process_element",
                                    inputDataConverter.toExternal(value));
            //            return true;
        } else {
            results =
                    (PyIterator)
                            interpreter.invokeMethod(
                                    "operation",
                                    "my_process_element",
                                    inputDataConverter.toExternal(value));
        }
        boolean result = false;
        //        System.out.println((Row) outputDataConverter.toInternal(results.next()));
        if (results.hasNext()) {
            Object obj = outputDataConverter.toInternal(results.next());
            try {

                result = (Long) ((Row) obj).getField(0) == 1;
            } catch (RuntimeException e) {
                throw new RuntimeException(e.getMessage() + "cast fail: " + obj);
            }
        }

        return result;
    }

    @Override
    public String toString() {
        return "SimplePythonCondition{" + "patternName='" + patternName + '\'' + '}';
    }
}
