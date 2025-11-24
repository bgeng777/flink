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

package org.apache.flink.model.python;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.PythonPredictFunction;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PythonPredictRuntimeProvider;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link ModelProviderFactory} for OpenAI model functions. */
public class GenericPythonModelProviderFactory implements ModelProviderFactory {
    public static final String IDENTIFIER = "generic-python";
    private static final String propertiesPrefix = "properties.";

    @Override
    public ModelProvider createModelProvider(Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validateExcept(propertiesPrefix);
        String model = helper.getOptions().get(GenericPythonOptions.MODEL);
        String pythonClass = helper.getOptions().get(GenericPythonOptions.PYTHON_PREDICT_FUNCTION);
        Map<String, String> modelConfig =
                helper.getOptions().toMap().entrySet().stream()
                        .filter(e -> e.getKey() != null && e.getKey().startsWith(propertiesPrefix))
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().substring(propertiesPrefix.length()),
                                        Map.Entry::getValue));
        PythonPredictFunction function =
                new GenericPythonPredictFunction(model, pythonClass, modelConfig);

        return new Provider(function);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(GenericPythonOptions.MODEL);
        set.add(GenericPythonOptions.PYTHON_PREDICT_FUNCTION);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    /** {@link ModelProvider} for python model functions. */
    public static class Provider implements PythonPredictRuntimeProvider {
        private final PythonPredictFunction function;

        public Provider(PythonPredictFunction function) {
            this.function = function;
        }

        @Override
        public ModelProvider copy() {
            return new Provider(function);
        }

        @Override
        public PythonPredictFunction createPythonPredictFunction(Context context) {
            return function;
        }
    }
}
