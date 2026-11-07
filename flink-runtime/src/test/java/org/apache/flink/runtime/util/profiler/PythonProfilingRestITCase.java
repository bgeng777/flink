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

package org.apache.flink.runtime.util.profiler;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.cluster.ProfilingRequestBody;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorBuilder;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesBuilder;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.FlinkExpectedException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the actual REST -> RM -> TM and existing BLOB download route using a fixture sampler.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class PythonProfilingRestITCase {
    @TempDir private Path temporary;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client = HttpClient.newHttpClient();

    private JsonNode get(URI base, String path) throws Exception {
        final HttpResponse<String> response =
                client.send(
                        HttpRequest.newBuilder(base.resolve(path))
                                .timeout(Duration.ofSeconds(10))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return mapper.readTree(response.body());
    }

    private boolean taskManagersRegistered(URI base, int expectedCount) throws Exception {
        final HttpResponse<String> response =
                client.send(
                        HttpRequest.newBuilder(base.resolve("/taskmanagers"))
                                .timeout(Duration.ofSeconds(10))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200
                && mapper.readTree(response.body()).path("taskmanagers").size() == expectedCount;
    }

    private HttpResponse<String> post(URI base, String path, Map<String, Object> body)
            throws Exception {
        body = new java.util.HashMap<>(body);
        if (body.containsKey("workerId")) {
            body.putIfAbsent("type", "CPU");
        }
        return client.send(
                HttpRequest.newBuilder(base.resolve(path))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @Timeout(60)
    void usesExistingProfilingEndpointsAndDownload() throws Exception {
        final Path sampler = temporary.resolve("sampler");
        final Path arguments = temporary.resolve("arguments");
        final Path release = temporary.resolve("release");
        Files.writeString(
                sampler,
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '"
                        + arguments
                        + "'\nwhile [ \"$#\" -gt 0 ]; do\n"
                        + "if [ \"$1\" = --output ]; then shift; output=$1; fi\nshift\ndone\n"
                        + "while [ ! -e '"
                        + release
                        + "' ]; do sleep 0.05; done\n"
                        + "printf '<svg>TEST FIXTURE, NOT A SAMPLED PROFILE</svg>' > \"$output\"\necho 'Samples: 1 Errors: 0'\n");
        Files.setPosixFilePermissions(sampler, PosixFilePermissions.fromString("rwx------"));
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.ENABLE_PROFILER, true);
        configuration.setString("rest.python-profiling.cpu.executable", sampler.toString());
        configuration.set(
                RestOptions.PROFILING_RESULT_DIR, temporary.resolve("results").toString());
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, temporary.toString());
        final Process worker = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try (MiniCluster cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(1)
                                .build())) {
            cluster.start();
            final URI base = cluster.getRestAddress().get(20, TimeUnit.SECONDS);
            waitUntilCondition(() -> taskManagersRegistered(base, 1), 20L, 500);
            final String tm =
                    get(base, "/taskmanagers").path("taskmanagers").get(0).path("id").asText();
            final String prefix = "/taskmanagers/" + tm;
            assertThat(get(base, prefix + "/python-workers").path("workers").isEmpty()).isTrue();
            final String id = registerWorker(worker, 0);
            assertThat(
                            get(base, prefix + "/python-workers")
                                    .path("workers")
                                    .get(0)
                                    .path("id")
                                    .asText())
                    .isEqualTo(id);
            assertThat(
                            post(
                                            base,
                                            prefix + "/profiler",
                                            Map.of("workerId", "missing", "duration", 1))
                                    .statusCode())
                    .isEqualTo(404);
            assertThat(
                            post(
                                            base,
                                            prefix + "/profiler",
                                            Map.of("workerId", id, "mode", "CPU", "duration", 1))
                                    .statusCode())
                    .isEqualTo(400);
            assertThat(
                            post(
                                            base,
                                            "/jobmanager/profiler",
                                            Map.of("workerId", id, "duration", 1))
                                    .statusCode())
                    .isEqualTo(400);
            assertThat(
                            post(
                                            base,
                                            prefix + "/profiler",
                                            Map.of(
                                                    "mode",
                                                    "CPU",
                                                    "duration",
                                                    1,
                                                    "includeNative",
                                                    true))
                                    .statusCode())
                    .isEqualTo(400);
            assertThat(
                            post(
                                            base,
                                            "/jobmanager/profiler",
                                            Map.of(
                                                    "mode",
                                                    "CPU",
                                                    "duration",
                                                    1,
                                                    "includeNative",
                                                    true))
                                    .statusCode())
                    .isEqualTo(400);
            for (Map<String, Object> invalid :
                    List.<Map<String, Object>>of(
                            Map.of("workerId", id, "duration", 0),
                            Map.of("workerId", id, "duration", 301),
                            Map.of("workerId", id, "duration", 1.5),
                            Map.of("workerId", 123, "duration", 1),
                            Map.of("workerId", id, "duration", 1, "includeNative", "false"),
                            Map.of("workerId", id, "duration", 1, "includeIdle", 1),
                            Map.of("mode", "CPU", "duration", 1, "includeIdle", false))) {
                assertThat(post(base, prefix + "/profiler", invalid).statusCode()).isEqualTo(400);
            }
            for (String query : List.of("unknown", "jvm,python")) {
                assertThat(
                                client.send(
                                                HttpRequest.newBuilder(
                                                                base.resolve(
                                                                        prefix
                                                                                + "/profiler?target="
                                                                                + query))
                                                        .GET()
                                                        .build(),
                                                HttpResponse.BodyHandlers.ofString())
                                        .statusCode())
                        .isEqualTo(400);
            }
            final HttpResponse<String> accepted =
                    post(
                            base,
                            prefix + "/profiler",
                            Map.of(
                                    "workerId",
                                    id,
                                    "duration",
                                    1,
                                    "includeNative",
                                    true,
                                    "includeIdle",
                                    true));
            assertThat(accepted.statusCode()).isEqualTo(200);
            assertThat(mapper.readTree(accepted.body()).path("worker").size()).isEqualTo(4);
            assertThat(mapper.readTree(accepted.body()).path("worker").path("id").asText())
                    .isEqualTo(id);
            assertThat(
                            post(base, prefix + "/profiler", Map.of("workerId", id, "duration", 1))
                                    .statusCode())
                    .isEqualTo(409);
            Files.writeString(release, "finish capture");
            waitUntilCondition(
                    () ->
                            get(base, prefix + "/profiler?target=python")
                                    .path("profilingList")
                                    .get(0)
                                    .path("status")
                                    .asText()
                                    .equals("FINISHED"),
                    20L,
                    500);
            assertThat(get(base, prefix + "/profiler").path("profilingList")).isEmpty();
            final JsonNode result =
                    get(base, prefix + "/profiler?target=python").path("profilingList").get(0);
            assertThat(result.path("includeNative").asBoolean()).isTrue();
            assertThat(result.path("includeIdle").asBoolean()).isTrue();
            assertThat(Files.readAllLines(arguments)).contains("--native", "--idle");
            final String file = result.path("outputFile").asText();
            assertThat(file).startsWith("python-").endsWith(".svg").doesNotContain("/");
            final HttpResponse<String> download =
                    client.send(
                            HttpRequest.newBuilder(base.resolve(prefix + "/profiler/" + file))
                                    .GET()
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            assertThat(download.statusCode()).isEqualTo(200);
            assertThat(download.headers().firstValue("Content-Type")).contains("image/svg+xml");
            assertThat(download.headers().firstValue("Content-Disposition")).contains("attachment");
            assertThat(download.body()).contains("TEST FIXTURE");
            assertThat(download.body())
                    .isEqualTo(Files.readString(temporary.resolve("results").resolve(file)));
            final String pendingFile = "python-memory-test.bin.inprogress";
            Files.writeString(temporary.resolve("results").resolve(pendingFile), "incomplete");
            for (String route : List.of(prefix + "/profiler/", "/jobmanager/profiler/")) {
                for (String suffix :
                        List.of("inprogress", "inprogress%2F", "INPROGRESS", "INPROGRESS%2F")) {
                    assertThat(
                                    client.send(
                                                    HttpRequest.newBuilder(
                                                                    base.resolve(
                                                                            route
                                                                                    + "python-memory-test.bin."
                                                                                    + suffix))
                                                            .GET()
                                                            .build(),
                                                    HttpResponse.BodyHandlers.ofString())
                                            .statusCode())
                            .isEqualTo(404);
                }
            }
        } finally {
            worker.destroyForcibly();
            worker.waitFor(5, TimeUnit.SECONDS);
            ProfilingService.getInstance(configuration).close();
        }
    }

    private String registerWorker(Process worker, int taskManagerIndex) throws Exception {
        final Path workingDirectory;
        try (Stream<Path> directories = Files.list(temporary)) {
            workingDirectory =
                    directories
                            .filter(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .startsWith("minicluster_"))
                            .findFirst()
                            .orElseThrow()
                            .resolve("tm_" + taskManagerIndex + "/tmp");
        }
        return registerWorker(worker, workingDirectory);
    }

    private String registerWorker(Process worker, Path workingDirectory) throws Exception {
        final Path registry =
                PythonWorkerRegistry.getRegistrationDirectory(workingDirectory.toFile());
        Files.createDirectories(registry);
        final String id = UUID.randomUUID().toString().replace("-", "");
        mapper.writeValue(
                registry.resolve(id + ".json").toFile(),
                Map.of(
                        "registration_id",
                        id,
                        "worker_id",
                        "rest-fixture",
                        "pid",
                        worker.pid(),
                        "start_token",
                        PythonWorkerRegistry.readStartToken(worker.pid())));
        return id;
    }

    @Test
    @Timeout(30)
    void rejectsPythonCaptureWhenWorkerLookupCompletesDuringShutdown() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.ENABLE_PROFILER, true);
        configuration.set(
                RestOptions.PYTHON_CPU_PROFILING_EXECUTABLE,
                temporary.resolve("unused-sampler").toString());
        configuration.set(
                RestOptions.PROFILING_RESULT_DIR, temporary.resolve("results").toString());
        final WorkingDirectory workingDirectory = WorkingDirectory.create(temporary.toFile());
        final TestingRpcService rpc = new TestingRpcService();
        final TestingHighAvailabilityServices ha = new TestingHighAvailabilityServices();
        ha.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
        final ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        final CompletableFuture<Void> slotsClosed = new CompletableFuture<>();
        final TaskExecutor taskExecutor =
                TaskExecutorBuilder.newBuilder(rpc, ha, workingDirectory)
                        .setConfiguration(configuration)
                        .setTaskManagerServices(
                                new TaskManagerServicesBuilder()
                                        .setIoExecutor(io)
                                        .setTaskSlotTable(
                                                TestingTaskSlotTable.<Task>newBuilder()
                                                        .closeAsyncReturns(slotsClosed)
                                                        .build())
                                        .build())
                        .build();
        final ProfilingService service = ProfilingService.getInstance(configuration);
        final Process worker = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try {
            taskExecutor.start();
            final String id = registerWorker(worker, workingDirectory.getTmpDirectory().toPath());
            final CompletableFuture<ProfilingInfo> request =
                    taskExecutor
                            .getSelfGateway(TaskExecutorGateway.class)
                            .requestProfiling(
                                    new ProfilingRequestBody(null, 30, id, false, false),
                                    Duration.ofSeconds(10));
            waitUntilCondition(() -> io.numQueuedRunnables() == 1, 20L, 500);
            final CompletableFuture<Void> termination = taskExecutor.closeAsync();
            taskExecutor
                    .getSelfGateway(RpcServer.class)
                    .callAsync(() -> null, Duration.ofSeconds(5))
                    .get(5, TimeUnit.SECONDS);
            assertThat(termination).isNotDone();

            io.triggerAll();
            assertThatThrownBy(() -> request.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(FlinkExpectedException.class)
                    .hasRootCauseMessage("The TaskExecutor is shutting down.");
            assertThat(
                            service.getProfilingList(
                                            taskExecutor.getResourceID().getResourceIdString())
                                    .get())
                    .isEmpty();
            assertThat(temporary.resolve("results")).doesNotExist();
        } finally {
            slotsClosed.complete(null);
            taskExecutor.closeAsync().get(10, TimeUnit.SECONDS);
            RpcUtils.terminateRpcService(rpc);
            service.close();
            worker.destroyForcibly();
            assertThat(worker.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @Timeout(60)
    void taskManagerShutdownReapsOnlyItsOwnPythonSampler() throws Exception {
        final Path sampler = temporary.resolve("sampler");
        final Path samplerPid = temporary.resolve("sampler.pid");
        Files.writeString(sampler, "#!/bin/sh\necho $$ > '" + samplerPid + "'\nexec sleep 60\n");
        Files.setPosixFilePermissions(sampler, PosixFilePermissions.fromString("rwx------"));
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.ENABLE_PROFILER, true);
        configuration.set(RestOptions.PYTHON_CPU_PROFILING_EXECUTABLE, sampler.toString());
        configuration.set(
                RestOptions.PROFILING_RESULT_DIR, temporary.resolve("results").toString());
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, temporary.toString());
        final ProfilingService service = ProfilingService.getInstance(configuration);
        final Process worker = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try (MiniCluster cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(2)
                                .setNumSlotsPerTaskManager(1)
                                .build())) {
            cluster.start();
            final URI base = cluster.getRestAddress().get(20, TimeUnit.SECONDS);
            waitUntilCondition(() -> taskManagersRegistered(base, 2), 20L, 500);
            final String workerId = registerWorker(worker, 0);
            String owner = null;
            for (JsonNode taskManager : get(base, "/taskmanagers").path("taskmanagers")) {
                final String id = taskManager.path("id").asText();
                if (!get(base, "/taskmanagers/" + id + "/python-workers")
                        .path("workers")
                        .isEmpty()) {
                    owner = id;
                }
            }
            assertThat(owner).isNotNull();
            assertThat(
                            post(
                                            base,
                                            "/taskmanagers/" + owner + "/profiler",
                                            Map.of("workerId", workerId, "duration", 30))
                                    .statusCode())
                    .isEqualTo(200);
            waitUntilCondition(
                    () -> Files.exists(samplerPid) && Files.size(samplerPid) > 0, 20L, 500);
            final ProcessHandle process =
                    ProcessHandle.of(Long.parseLong(Files.readString(samplerPid).trim()))
                            .orElseThrow();

            cluster.terminateTaskManager(1).get(20, TimeUnit.SECONDS);
            assertThat(process.isAlive()).isTrue();
            assertThat(service.getProfilingFuture().isDone()).isFalse();

            cluster.terminateTaskManager(0).get(20, TimeUnit.SECONDS);
            assertThat(process.isAlive()).isFalse();
            assertThat(worker.isAlive()).isTrue();
            assertThat(service.getProfilingFuture().isDone()).isTrue();
            assertThat(service.getProfilingList(owner).get())
                    .singleElement()
                    .satisfies(
                            info -> {
                                assertThat(info.getStatus())
                                        .isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
                                assertThat(info.getMessage()).contains("cancelled");
                            });
            assertThat(ProfilingService.getInstance(configuration)).isSameAs(service);
            try (Stream<Path> artifacts = Files.list(temporary.resolve("results"))) {
                assertThat(artifacts).isEmpty();
            }
        } finally {
            service.close();
            if (Files.exists(samplerPid) && Files.size(samplerPid) > 0) {
                ProcessHandle.of(Long.parseLong(Files.readString(samplerPid).trim()))
                        .filter(ProcessHandle::isAlive)
                        .ifPresent(ProcessHandle::destroyForcibly);
            }
            worker.destroyForcibly();
            assertThat(worker.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @Timeout(60)
    void unavailablePythonProfilingDoesNotDisableJvmHistory() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.ENABLE_PROFILER, true);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(
                RestOptions.PROFILING_RESULT_DIR, temporary.resolve("results").toString());
        try (MiniCluster cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(1)
                                .build())) {
            cluster.start();
            final URI base = cluster.getRestAddress().get(20, TimeUnit.SECONDS);
            waitUntilCondition(() -> taskManagersRegistered(base, 1), 20L, 500);
            final String tm =
                    get(base, "/taskmanagers").path("taskmanagers").get(0).path("id").asText();
            final String prefix = "/taskmanagers/" + tm;
            assertThat(
                            client.send(
                                            HttpRequest.newBuilder(
                                                            base.resolve(
                                                                    prefix + "/python-workers"))
                                                    .GET()
                                                    .build(),
                                            HttpResponse.BodyHandlers.ofString())
                                    .statusCode())
                    .isEqualTo(200);
            assertThat(
                            post(
                                            base,
                                            prefix + "/profiler",
                                            Map.of("workerId", "missing", "duration", 1))
                                    .statusCode())
                    .isEqualTo(503);
            assertThat(get(base, prefix + "/profiler").path("profilingList")).isEmpty();
        } finally {
            ProfilingService.getInstance(configuration).close();
        }
    }
}
