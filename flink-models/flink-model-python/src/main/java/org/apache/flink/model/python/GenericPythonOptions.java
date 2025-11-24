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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

/** Options for OpenAI API Model Functions. */
@Experimental
public class GenericPythonOptions {

    // ------------------------------------------------------------------------
    //  Common Options
    // ------------------------------------------------------------------------

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<String> MODEL =
            ConfigOptions.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder().text("Model path or model name").build());

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<String> PYTHON_PREDICT_FUNCTION =
            ConfigOptions.key("python-predict-function")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(Description.builder().text("PYTHON_PREDICT_FUNCTION").build());
}
