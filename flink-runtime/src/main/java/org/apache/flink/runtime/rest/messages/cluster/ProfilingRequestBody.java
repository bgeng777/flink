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

package org.apache.flink.runtime.rest.messages.cluster;

import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link RequestBody} to trigger profiling instance. */
public class ProfilingRequestBody implements RequestBody, Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_NAME_MODE = "mode";
    private static final String FIELD_NAME_DURATION = "duration";

    @JsonProperty(FIELD_NAME_MODE)
    private final ProfilingInfo.ProfilingMode mode;

    @JsonProperty(FIELD_NAME_DURATION)
    private final int duration;

    @JsonProperty("workerId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String workerId;

    @JsonProperty("includeIdle")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Boolean includeIdle;

    @JsonProperty("includeNative")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Boolean includeNative;

    public ProfilingRequestBody(ProfilingInfo.ProfilingMode mode, int duration) {
        this(mode, duration, null, null, null);
    }

    public ProfilingRequestBody(
            @JsonProperty(FIELD_NAME_MODE) final ProfilingInfo.ProfilingMode mode,
            @JsonProperty(FIELD_NAME_DURATION) final int duration,
            @JsonProperty("workerId") final String workerId,
            @JsonProperty("includeIdle") final Boolean includeIdle,
            @JsonProperty("includeNative") final Boolean includeNative) {
        this.mode = mode;
        this.duration = duration;
        this.workerId = workerId;
        this.includeIdle = includeIdle;
        this.includeNative = includeNative;
    }

    @JsonCreator
    public static ProfilingRequestBody fromJson(
            @JsonProperty(FIELD_NAME_MODE) ProfilingInfo.ProfilingMode mode,
            @JsonProperty(FIELD_NAME_DURATION) JsonNode duration,
            @JsonProperty("workerId") JsonNode workerId,
            @JsonProperty("includeIdle") JsonNode includeIdle,
            @JsonProperty("includeNative") JsonNode includeNative) {
        checkArgument(
                duration != null && duration.isIntegralNumber() && duration.canConvertToInt(),
                "duration must be an integer number of seconds.");
        checkArgument(
                workerId == null || workerId.isNull() || workerId.isTextual(),
                "workerId must be a string.");
        return new ProfilingRequestBody(
                mode,
                duration.intValue(),
                workerId == null || workerId.isNull() ? null : workerId.textValue(),
                parseBoolean(includeIdle, "includeIdle"),
                parseBoolean(includeNative, "includeNative"));
    }

    private static Boolean parseBoolean(JsonNode value, String name) {
        if (value == null) {
            return null;
        }
        checkArgument(value.isBoolean(), "%s must be a boolean.", name);
        return value.booleanValue();
    }

    @JsonIgnore
    public ProfilingInfo.ProfilingMode getMode() {
        return mode;
    }

    @JsonIgnore
    public String getWorkerId() {
        return workerId;
    }

    @JsonIgnore
    public boolean isIncludeIdle() {
        return Boolean.TRUE.equals(includeIdle);
    }

    @JsonIgnore
    public boolean isIncludeNative() {
        return Boolean.TRUE.equals(includeNative);
    }

    @JsonIgnore
    public boolean hasPythonOptions() {
        return includeIdle != null || includeNative != null;
    }

    @JsonIgnore
    public int getDuration() {
        return duration;
    }
}
