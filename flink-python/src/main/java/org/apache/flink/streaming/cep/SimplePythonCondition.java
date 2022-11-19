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
    private final PythonInterpreter interpreter;
    private final PythonTypeUtils.DataConverter<T, Object> inputDataConverter;

    public SimplePythonCondition(
            PythonInterpreter interpreter,
            PythonTypeUtils.DataConverter<T, Object> inputDataConverter) {
        this.interpreter = interpreter;
        this.inputDataConverter = inputDataConverter;
    }

    @Override
    public boolean filter(T value) throws Exception {
        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod(
                                "condition", "filter", inputDataConverter.toExternal(value));
        boolean result = false;
        if (results.hasNext()) {
            result = (boolean) results.next();
        }

        return result;
    }
}
