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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** Public identity of a Python worker, which may serve multiple operators. */
public class PythonWorkerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("id")
    private final String id;

    @JsonProperty("pid")
    private final long pid;

    @JsonCreator
    public PythonWorkerInfo(@JsonProperty("id") String id, @JsonProperty("pid") long pid) {
        this.id = id;
        this.pid = pid;
    }

    public String getId() {
        return id;
    }

    public long getPid() {
        return pid;
    }
}
