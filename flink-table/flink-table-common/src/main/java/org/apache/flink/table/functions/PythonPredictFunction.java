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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.utils.PythonFunctionUtils;

/**
 * A wrapper class of {@link TableFunction} for synchronous model inference.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 */
@PublicEvolving
public abstract class PythonPredictFunction extends TableFunction<RowData> {

    public abstract String getPythonClass();

    public abstract Configuration getModelConfig();

    /**
     * Create PythonFunctionInfo for this prediction function.
     *
     * @return PythonFunctionInfo configured for this prediction function
     */
    @Internal
    public PythonFunction createPythonFunction(Configuration pythonEnvConfig) {
        return PythonFunctionUtils.getPythonFunction(
                getPythonClass(), pythonEnvConfig, this.getClass().getClassLoader());
    }

    public final void eval(Object... args) {
        throw new IllegalStateException("This method is a placeholder and should not be called.");
    }
}
