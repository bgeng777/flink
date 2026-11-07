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
import java.util.Collections;
import java.util.List;

/** Public identity of a Python worker, which may serve multiple operators. */
public class PythonWorkerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("id")
    private final String id;

    @JsonProperty("pid")
    private final long pid;

    @JsonProperty("memoryProfilingAvailable")
    private final boolean memoryProfilingAvailable;

    @JsonProperty("operators")
    private final List<Operator> operators;

    public PythonWorkerInfo(String id, long pid) {
        this(id, pid, false);
    }

    public PythonWorkerInfo(String id, long pid, boolean memoryProfilingAvailable) {
        this(id, pid, memoryProfilingAvailable, Collections.emptyList());
    }

    @JsonCreator
    public PythonWorkerInfo(
            @JsonProperty("id") String id,
            @JsonProperty("pid") long pid,
            @JsonProperty("memoryProfilingAvailable") boolean memoryProfilingAvailable,
            @JsonProperty("operators") List<Operator> operators) {
        this.id = id;
        this.pid = pid;
        this.memoryProfilingAvailable = memoryProfilingAvailable;
        this.operators = List.copyOf(operators);
    }

    public List<Operator> getOperators() {
        return operators;
    }

    public boolean isMemoryProfilingAvailable() {
        return memoryProfilingAvailable;
    }

    public String getId() {
        return id;
    }

    public long getPid() {
        return pid;
    }

    /** An operator instance using this worker; the profile still covers the whole process. */
    public static final class Operator implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("jobId")
        private final String jobId;

        @JsonProperty("operatorId")
        private final String operatorId;

        @JsonProperty("operatorName")
        private final String operatorName;

        @JsonProperty("subtaskIndex")
        private final int subtaskIndex;

        @JsonProperty("attemptNumber")
        private final int attemptNumber;

        @JsonCreator
        public Operator(
                @JsonProperty("jobId") String jobId,
                @JsonProperty("operatorId") String operatorId,
                @JsonProperty("operatorName") String operatorName,
                @JsonProperty("subtaskIndex") int subtaskIndex,
                @JsonProperty("attemptNumber") int attemptNumber) {
            this.jobId = jobId;
            this.operatorId = operatorId;
            this.operatorName = operatorName;
            this.subtaskIndex = subtaskIndex;
            this.attemptNumber = attemptNumber;
        }

        public String getJobId() {
            return jobId;
        }

        public String getOperatorId() {
            return operatorId;
        }

        public String getOperatorName() {
            return operatorName;
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }
    }
}
