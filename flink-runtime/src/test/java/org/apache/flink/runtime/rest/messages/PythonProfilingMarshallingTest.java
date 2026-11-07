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

import org.apache.flink.runtime.rest.messages.cluster.ProfilingRequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the existing JVM JSON contract while carrying optional Python targets over REST and RPC.
 */
class PythonProfilingMarshallingTest {
    private final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

    @Test
    void preservesLegacyRequest() throws Exception {
        final String json = "{\"mode\":\"ITIMER\",\"duration\":10}";
        final ProfilingRequestBody request = mapper.readValue(json, ProfilingRequestBody.class);
        assertThat(request.getMode()).isEqualTo(ProfilingInfo.ProfilingMode.ITIMER);
        assertThat(request.getWorkerId()).isNull();
        assertThat(request.isIncludeNative()).isFalse();
        assertThat(mapper.readTree(mapper.writeValueAsString(request)))
                .isEqualTo(mapper.readTree(json));
    }

    @Test
    void carriesPythonTargetOverRestAndRpc() throws Exception {
        final ProfilingRequestBody request =
                mapper.readValue(
                        "{\"type\":\"CPU\",\"workerId\":\"abc\",\"duration\":10,\"includeIdle\":true,\"includeNative\":true}",
                        ProfilingRequestBody.class);
        assertThat(request.getMode()).isNull();
        assertThat(request.getWorkerId()).isEqualTo("abc");
        final ProfilingRequestBody rpcCopy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(request), getClass().getClassLoader());
        assertThat(rpcCopy.getWorkerId()).isEqualTo("abc");
        assertThat(rpcCopy.isIncludeIdle()).isTrue();
        assertThat(rpcCopy.isIncludeNative()).isTrue();
    }

    @Test
    void preservesLegacyResponseFields() throws Exception {
        final ProfilingInfo info = ProfilingInfo.create(10, ProfilingInfo.ProfilingMode.ITIMER);
        info.success("jvm.html");
        final JsonNode json = mapper.readTree(mapper.writeValueAsString(info));
        assertThat(json.has("worker")).isFalse();
        assertThat(json.has("includeIdle")).isFalse();
        assertThat(json.has("includeNative")).isFalse();
        assertThat(json.path("status").asText()).isEqualTo("FINISHED");
        assertThat(json.path("profilingMode").asText()).isEqualTo("ITIMER");
        assertThat(json.size()).isEqualTo(8);
    }

    @Test
    void roundTripsPythonResponseMetadata() throws Exception {
        final PythonWorkerInfo.Operator operator =
                new PythonWorkerInfo.Operator("job", "operator", "infer-images", 2, 1);
        final ProfilingInfo info =
                ProfilingInfo.createPython(
                        10,
                        new PythonWorkerInfo("id", 123, false, Collections.singletonList(operator)),
                        true,
                        true);
        info.success("python-test.svg");
        final JsonNode responseJson = mapper.readTree(mapper.writeValueAsString(info));
        assertThat(responseJson.has("mode")).isFalse();
        assertThat(responseJson.has("profilingMode")).isFalse();
        final JsonNode workerJson = responseJson.path("worker");
        assertThat(workerJson.size()).isEqualTo(4);
        assertThat(workerJson.has("beamWorkerId")).isFalse();
        assertThat(workerJson.has("startToken")).isFalse();
        final ProfilingInfo decoded =
                RestMapperUtils.getFlexibleObjectMapper()
                        .readValue(mapper.writeValueAsString(info), ProfilingInfo.class);
        assertThat(decoded.getWorker().getPid()).isEqualTo(123);
        assertThat(decoded.getWorker().getId()).isEqualTo("id");
        assertThat(decoded.getWorker().getOperators())
                .singleElement()
                .usingRecursiveComparison()
                .isEqualTo(operator);
        assertThat(decoded.getIncludeIdle()).isTrue();
        assertThat(decoded.getIncludeNative()).isTrue();
        assertThat(decoded.getProfilingMode()).isNull();
        assertThat(decoded.getOutputFile()).isEqualTo("python-test.svg");
    }
}
