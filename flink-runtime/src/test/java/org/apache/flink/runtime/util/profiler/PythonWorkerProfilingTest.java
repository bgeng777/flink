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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.util.profiler.PythonWorkerRegistry.Worker;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real child processes verify the profiling lifecycle without requiring ptrace privileges. */
@EnabledOnOs({OS.LINUX, OS.MAC})
@Timeout(30)
class PythonWorkerProfilingTest {
    @TempDir private Path temporary;
    private Process workerProcess;
    private Worker worker;
    private PythonWorkerRegistry registry;
    private ProfilingService service;
    private Path sampler;
    private Path output;

    @BeforeEach
    void setUp() throws Exception {
        workerProcess = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        registry = new PythonWorkerRegistry(temporary.toFile());
        worker = registerWorker(workerProcess);
        sampler = temporary.resolve("sampler");
        output = temporary.resolve("results");
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.PROFILING_RESULT_DIR, output.toString());
        configuration.set(RestOptions.MAX_PROFILING_HISTORY_SIZE, 2);
        service = ProfilingService.getInstance(configuration);
    }

    private Worker registerWorker(Process process) throws Exception {
        final Worker registered =
                new Worker(
                        UUID.randomUUID().toString().replace("-", ""),
                        process.pid(),
                        PythonWorkerRegistry.readStartToken(process.pid()));
        final Path directory = PythonWorkerRegistry.getRegistrationDirectory(temporary.toFile());
        Files.createDirectories(directory);
        new ObjectMapper()
                .writeValue(
                        directory.resolve(registered.getId() + ".json").toFile(),
                        Map.of(
                                "registration_id", registered.getId(),
                                "worker_id", "beam-worker",
                                "pid", registered.getPid(),
                                "start_token", registered.getStartToken()));
        return registered;
    }

    @AfterEach
    void tearDown() throws Exception {
        service.close();
        workerProcess.destroyForcibly();
        assertThat(workerProcess.waitFor(5, TimeUnit.SECONDS)).isTrue();
    }

    private void sampler(String body) throws Exception {
        Files.writeString(
                sampler,
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '"
                        + temporary.resolve("arguments")
                        + "'\nwhile [ \"$#\" -gt 0 ]; do\n"
                        + "  if [ \"$1\" = --output ]; then shift; output=$1; fi\n"
                        + "  shift\ndone\n"
                        + body
                        + "\necho 'Samples: 1 Errors: 0'\n");
        Files.setPosixFilePermissions(sampler, PosixFilePermissions.fromString("rwx------"));
    }

    private ProfilingInfo start() throws Exception {
        return service.requestPythonProfiling(
                        "tm", 2, worker, registry, sampler.toString(), false, false)
                .get();
    }

    private void awaitCompletion() throws Exception {
        waitUntilCondition(() -> service.getProfilingFuture().isDone(), 20L, 500);
    }

    @Test
    void registersOnlyLivingDescendantsWithMatchingStartToken() throws Exception {
        assertThat(registry.getWorker(worker.getId()).getPid()).isEqualTo(workerProcess.pid());
        assertThat(registry.isAlive(new Worker(worker.getId(), worker.getPid(), "stale")))
                .isFalse();
        assertThat(
                        registry.isAlive(
                                new Worker(
                                        worker.getId(),
                                        ProcessHandle.current().pid(),
                                        PythonWorkerRegistry.readStartToken(
                                                ProcessHandle.current().pid()))))
                .isFalse();
        workerProcess.destroyForcibly();
        workerProcess.waitFor();
        assertThat(registry.listWorkers()).isEmpty();
    }

    @Test
    void usesExistingHistoryAndResultDirectory() throws Exception {
        sampler("printf '<svg>TEST FIXTURE, NOT SAMPLED</svg>' > \"$output\"");
        Files.createDirectories(output);
        final Path unrelated = output.resolve("keep.txt");
        Files.writeString(unrelated, "unrelated");
        final ProfilingInfo first = start();
        awaitCompletion();
        assertThat(first.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
        assertThat(first.getWorker().getId()).isEqualTo(worker.getId());
        assertThat(first.getOutputFile()).startsWith("python-").endsWith(".svg");
        assertThat(output.resolve(first.getOutputFile())).isRegularFile();
        start();
        awaitCompletion();
        start();
        awaitCompletion();
        assertThat(service.getProfilingList("tm").get()).hasSize(2);
        assertThat(output.resolve(first.getOutputFile())).doesNotExist();
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files.collect(Collectors.toList())).hasSize(3).contains(unrelated);
        }
    }

    @Test
    void zeroSamplesFailAndRemoveTheArtifact() throws Exception {
        sampler(
                "printf '<svg>EMPTY TEST FIXTURE</svg>' > \"$output\"; echo 'Samples: 0 Errors: 0'; exit 0");
        final ProfilingInfo info = start();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).contains("did not report any collected samples");
        assertThat(info.getOutputFile()).isNull();
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void outputDirectoryFailureDoesNotTerminateWorker() throws Exception {
        Files.writeString(output, "unrelated file");
        final ProfilingInfo info = start();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(workerProcess.isAlive()).isTrue();
        assertThat(Files.readString(output)).isEqualTo("unrelated file");
    }

    @Test
    void failureReleasesTheSharedCaptureSlot() throws Exception {
        final ProfilingInfo failed = start();
        awaitCompletion();
        assertThat(failed.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        final ProfilingInfo next = start();
        awaitCompletion();
        assertThat(next.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
    }

    @Test
    void preservesSamplerFailureMessage() throws Exception {
        sampler("echo 'Permission denied by operating system'; exit 1");
        final ProfilingInfo failed = start();
        awaitCompletion();
        assertThat(failed.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(failed.getMessage()).contains("Permission denied");
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files.collect(Collectors.toList())).isEmpty();
        }
    }

    private long startBlockingSampler() throws Exception {
        sampler(
                "echo partial > \"$output\"\necho $$ > '"
                        + temporary.resolve("ready")
                        + "'\nexec sleep 60");
        start();
        waitUntilCondition(() -> Files.exists(temporary.resolve("ready")), 20L, 250);
        return Long.parseLong(Files.readString(temporary.resolve("ready")).trim());
    }

    @Test
    void workerExitReapsAnAlreadyStartedSampler() throws Exception {
        final long samplerPid = startBlockingSampler();
        assertThat(ProcessHandle.of(samplerPid)).isPresent();
        workerProcess.destroyForcibly();
        workerProcess.waitFor();
        awaitCompletion();
        final ProfilingInfo info = service.getProfilingList("tm").get().iterator().next();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).contains("worker exited");
        assertThat(ProcessHandle.of(samplerPid).filter(ProcessHandle::isAlive)).isEmpty();
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files.collect(Collectors.toList())).isEmpty();
        }
    }

    @Test
    void pythonAndJvmShareConcurrencyLimit() throws Exception {
        startBlockingSampler();
        assertThatThrownBy(() -> start()).hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                service.requestProfiling(
                                                "tm", 1, ProfilingInfo.ProfilingMode.ITIMER)
                                        .get())
                .hasCauseInstanceOf(IllegalStateException.class);
        service.getProfilingFuture().cancel();
        awaitCompletion();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void passesRequestedSamplingOptions(boolean includeNative) throws Exception {
        sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        final ProfilingInfo info =
                service.requestPythonProfiling(
                                "tm", 2, worker, registry, sampler.toString(), true, includeNative)
                        .get();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
        assertThat(info.getIncludeNative()).isEqualTo(includeNative);
        assertThat(info.getIncludeIdle()).isTrue();
        assertThat(Files.readAllLines(temporary.resolve("arguments"))).contains("--idle");
        if (includeNative) {
            assertThat(Files.readAllLines(temporary.resolve("arguments"))).contains("--native");
        } else {
            assertThat(Files.readAllLines(temporary.resolve("arguments")))
                    .doesNotContain("--native");
        }
    }

    @Test
    void unsupportedNativeSamplingFailsWithoutFallback() throws Exception {
        sampler(
                "while read -r argument; do\n"
                        + "if [ \"$argument\" = --native ]; then echo 'unexpected argument --native'; exit 1; fi\n"
                        + "done < '"
                        + temporary.resolve("arguments")
                        + "'\n"
                        + "printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        final ProfilingInfo info =
                service.requestPythonProfiling(
                                "tm", 2, worker, registry, sampler.toString(), false, true)
                        .get();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getIncludeNative()).isTrue();
        assertThat(info.getMessage()).contains("unexpected argument --native");
        assertThat(info.getOutputFile()).isNull();
    }

    @Test
    void multipleWorkersKeepIndependentResults() throws Exception {
        final Process otherProcess = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try {
            final Worker otherWorker = registerWorker(otherProcess);
            assertThat(registry.listWorkers())
                    .extracting(Worker::getId)
                    .containsExactlyInAnyOrder(worker.getId(), otherWorker.getId());
            sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
            final ProfilingInfo first = start();
            awaitCompletion();
            final ProfilingInfo second =
                    service.requestPythonProfiling(
                                    "tm",
                                    2,
                                    registry.getWorker(otherWorker.getId()),
                                    registry,
                                    sampler.toString(),
                                    false,
                                    false)
                            .get();
            awaitCompletion();
            assertThat(first.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
            assertThat(second.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
            assertThat(second.getOutputFile()).isNotEqualTo(first.getOutputFile());
            assertThat(output.resolve(first.getOutputFile())).isRegularFile();
            assertThat(output.resolve(second.getOutputFile())).isRegularFile();
            assertThat(Files.readAllLines(temporary.resolve("arguments")))
                    .containsSubsequence("--pid", Long.toString(otherProcess.pid()));
            assertThat(service.getProfilingList("tm").get()).hasSize(2);
        } finally {
            otherProcess.destroyForcibly();
            assertThat(otherProcess.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void restartedWorkerRequiresANewHandle() throws Exception {
        workerProcess.destroyForcibly();
        workerProcess.waitFor();
        final Process replacement = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try {
            final Worker current = registerWorker(replacement);
            assertThat(registry.listWorkers())
                    .extracting(Worker::getId)
                    .containsExactly(current.getId());
            assertThatThrownBy(() -> registry.getWorker(worker.getId()))
                    .isInstanceOf(NoSuchElementException.class);
            assertThat(registry.getWorker(current.getId()).getPid()).isEqualTo(replacement.pid());
        } finally {
            replacement.destroyForcibly();
            assertThat(replacement.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void deadlineReapsAnUnresponsiveSampler() throws Exception {
        final long pid = startBlockingSampler();
        awaitCompletion();
        final ProfilingInfo info = service.getProfilingList("tm").get().iterator().next();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).contains("deadline");
        assertThat(ProcessHandle.of(pid).filter(ProcessHandle::isAlive)).isEmpty();
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files.collect(Collectors.toList())).isEmpty();
        }
    }

    @Test
    void cleanupFailureDoesNotReplaceSamplerFailure() throws Exception {
        sampler(
                "mkdir \"$output\"; echo partial > \"$output/partial\"; echo 'capture failed'; exit 1");
        final ProfilingInfo info = start();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).contains("capture failed");
        assertThat(info.getOutputFile()).isNull();
    }

    @Test
    void taskManagerCancellationWaitsForQueuedCapture() throws Exception {
        final CompletableFuture<Void> entered = new CompletableFuture<>();
        final CompletableFuture<Void> release = new CompletableFuture<>();
        service
        .new ProfilingFuture(
                0,
                () -> {
                    entered.complete(null);
                    release.join();
                });
        entered.get(5, TimeUnit.SECONDS);
        final ProfilingInfo info;
        final CompletableFuture<Void> cancellation;
        try {
            info = start();
            cancellation = service.cancelPythonProfiling("tm");
            assertThat(cancellation).isNotDone();
        } finally {
            release.complete(null);
        }
        cancellation.get(5, TimeUnit.SECONDS);
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).contains("cancelled");
        assertThat(output).doesNotExist();

        sampler("printf '<svg>TEST FIXTURE, NOT SAMPLED</svg>' > \"$output\"");
        final ProfilingInfo next = start();
        awaitCompletion();
        assertThat(next.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
    }

    @Test
    void completedPythonCaptureDoesNotCancelJvmCapture() throws Exception {
        sampler("printf '<svg>TEST FIXTURE, NOT SAMPLED</svg>' > \"$output\"");
        start();
        awaitCompletion();
        final ProfilingInfo jvm =
                service.requestProfiling("tm", 30, ProfilingInfo.ProfilingMode.ITIMER).get();
        assertThat(jvm.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.RUNNING);
        service.cancelPythonProfiling("tm").get(5, TimeUnit.SECONDS);
        assertThat(service.getProfilingFuture().isDone()).isFalse();
        assertThat(jvm.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.RUNNING);
    }

    @Test
    void shutdownCompletesQueuedCapture() throws Exception {
        final CompletableFuture<Void> entered = new CompletableFuture<>();
        final CompletableFuture<Void> release = new CompletableFuture<>();
        service
        .new ProfilingFuture(
                0,
                () -> {
                    entered.complete(null);
                    release.join();
                });
        entered.get(5, TimeUnit.SECONDS);
        final ProfilingInfo info;
        try {
            info = start();
            service.close();
        } finally {
            release.complete(null);
        }
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).contains("cancelled");
        assertThat(output).doesNotExist();
    }

    @Test
    void shutdownReapsSampler() throws Exception {
        final long pid = startBlockingSampler();
        service.close();
        awaitCompletion();
        assertThat(ProcessHandle.of(pid).filter(ProcessHandle::isAlive)).isEmpty();
    }
}
