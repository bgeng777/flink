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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

/**
 * PythonFunctionInfo contains the execution information of a Python function, such as: the actual
 * Python function, the input arguments, etc. It allows users to specify custom config for this
 * function as well. The config would be passed to the Python worker and can be accessed from the
 * FunctionContexts's get_job_parameter().
 */
@Internal
public class PythonFunctionInfoWithConfig extends PythonFunctionInfo {

    private static final long serialVersionUID = 1L;
    Configuration config;

    public PythonFunctionInfoWithConfig(
            PythonFunction pythonFunction, Object[] inputs, Configuration config) {
        super(pythonFunction, inputs);
        this.config = new Configuration(config);
    }

    public Configuration getConfig() {
        return config;
    }
}
