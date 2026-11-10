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
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.PythonWorkerInfo.Operator;
import org.apache.flink.runtime.util.profiler.PythonWorkerRegistry.Worker;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
                                "job_id", "job-a",
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
        waitUntilCondition(() -> service.getProfilingFuture().isDone(), 20L, 1000);
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
    void keepsOperatorAssociationsInMemory() throws Exception {
        registry.registerOperator(
                "beam-worker", "execution-a", new Operator("job-a", "a", "decode", 0, 0));
        assertThat(registry.getWorker(worker.getId()).toInfo().getOperators())
                .extracting(Operator::getOperatorName)
                .containsExactly("decode");
        assertThat(
                        PythonWorkerRegistry.getRegistrationDirectory(temporary.toFile())
                                .resolve("operators"))
                .doesNotExist();
    }

    @Test
    void removesOnlyTheClosingOperatorFromASharedWorker() throws Exception {
        final Runnable first =
                registry.registerOperator(
                        "beam-worker", "execution-a", new Operator("job-a", "a", "decode", 0, 0));
        final Runnable second =
                registry.registerOperator(
                        "beam-worker", "execution-b", new Operator("job-a", "b", "infer", 0, 1));
        assertThat(registry.getWorker(worker.getId()).toInfo().getOperators())
                .extracting(Operator::getOperatorName)
                .containsExactlyInAnyOrder("decode", "infer");
        first.run();
        first.run();
        assertThat(registry.getWorker(worker.getId()).toInfo().getOperators())
                .extracting(Operator::getOperatorName)
                .containsExactly("infer");
        second.run();
        assertThat(registry.getWorker(worker.getId()).toInfo().getOperators()).isEmpty();
        assertThat(workerProcess.isAlive()).isTrue();
    }

    @Test
    void closingAnOldAttemptPreservesTheNewAttempt() throws Exception {
        final Runnable oldAttempt =
                registry.registerOperator(
                        "beam-worker", "execution-a", new Operator("job-a", "a", "infer", 0, 0));
        final Runnable newAttempt =
                registry.registerOperator(
                        "beam-worker", "execution-b", new Operator("job-a", "a", "infer", 0, 1));
        oldAttempt.run();
        assertThat(registry.getWorker(worker.getId()).toInfo().getOperators())
                .extracting(Operator::getAttemptNumber)
                .containsExactly(1);
        newAttempt.run();
        assertThat(registry.getWorker(worker.getId()).toInfo().getOperators()).isEmpty();
    }

    @Test
    void sharesRegistryWithinOneTaskManagerAndReleasesItOnShutdown() throws Exception {
        final SharedResources resources = new SharedResources();
        final PythonWorkerRegistry first =
                PythonWorkerRegistry.getOrCreate(resources, temporary.toFile());
        try {
            first.registerOperator(
                    "beam-worker", "execution-a", new Operator("job-a", "a", "infer", 0, 0));
            final PythonWorkerRegistry second =
                    PythonWorkerRegistry.getOrCreate(resources, temporary.toFile());
            assertThat(second).isSameAs(first);
            assertThat(second.getWorker(worker.getId()).toInfo().getOperators())
                    .extracting(Operator::getOperatorName)
                    .containsExactly("infer");
        } finally {
            resources.release(PythonWorkerRegistry.SHARED_RESOURCE, resources, ignored -> {});
        }
        assertThat(first.getWorker(worker.getId()).toInfo().getOperators()).isEmpty();
    }

    @Test
    void keepsOperatorAssociationsSeparateAcrossTaskManagers() throws Exception {
        final SharedResources firstResources = new SharedResources();
        final SharedResources secondResources = new SharedResources();
        final Path otherDirectory = temporary.resolve("other-tm");
        final PythonWorkerRegistry first =
                PythonWorkerRegistry.getOrCreate(firstResources, temporary.toFile());
        final PythonWorkerRegistry second =
                PythonWorkerRegistry.getOrCreate(secondResources, otherDirectory.toFile());
        try {
            final Path workerFile = Path.of(worker.getId() + ".json");
            final Path otherRegistrations =
                    PythonWorkerRegistry.getRegistrationDirectory(otherDirectory.toFile());
            Files.createDirectories(otherRegistrations);
            Files.copy(
                    PythonWorkerRegistry.getRegistrationDirectory(temporary.toFile())
                            .resolve(workerFile),
                    otherRegistrations.resolve(workerFile));
            first.registerOperator(
                    "beam-worker", "execution-a", new Operator("job-a", "a", "decode", 0, 0));
            second.registerOperator(
                    "beam-worker", "execution-b", new Operator("job-a", "b", "infer", 0, 0));
            assertThat(second).isNotSameAs(first);
            assertThat(first.getWorker(worker.getId()).toInfo().getOperators())
                    .extracting(Operator::getOperatorName)
                    .containsExactly("decode");
            assertThat(second.getWorker(worker.getId()).toInfo().getOperators())
                    .extracting(Operator::getOperatorName)
                    .containsExactly("infer");
        } finally {
            firstResources.release(
                    PythonWorkerRegistry.SHARED_RESOURCE, firstResources, ignored -> {});
            secondResources.release(
                    PythonWorkerRegistry.SHARED_RESOURCE, secondResources, ignored -> {});
        }
    }

    @Test
    void separatesTheSameBeamWorkerIdInDifferentJobs() throws Exception {
        final Process secondProcess = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try {
            final Worker second = registerWorker(secondProcess);
            final Path registration =
                    PythonWorkerRegistry.getRegistrationDirectory(temporary.toFile())
                            .resolve(second.getId() + ".json");
            final ObjectMapper mapper = new ObjectMapper();
            final ObjectNode record = (ObjectNode) mapper.readTree(Files.readString(registration));
            record.put("job_id", "job-b");
            mapper.writeValue(registration.toFile(), record);
            registry.registerOperator(
                    "beam-worker", "execution-a", new Operator("job-a", "a", "decode", 0, 0));
            registry.registerOperator(
                    "beam-worker", "execution-b", new Operator("job-b", "b", "infer", 0, 0));
            assertThat(registry.getWorker(worker.getId()).toInfo().getOperators())
                    .extracting(Operator::getOperatorName)
                    .containsExactly("decode");
            assertThat(registry.getWorker(second.getId()).toInfo().getOperators())
                    .extracting(Operator::getOperatorName)
                    .containsExactly("infer");
        } finally {
            secondProcess.destroyForcibly();
            secondProcess.waitFor(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void retainsOnlyTheLatestSuccessfulResult() throws Exception {
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
        assertThat(service.getProfilingList("tm").get()).hasSize(1);
        assertThat(output.resolve(first.getOutputFile())).doesNotExist();
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files.collect(Collectors.toList())).hasSize(2).contains(unrelated);
        }
    }

    @Test
    void failedCapturePreservesThePreviousSuccessfulResult() throws Exception {
        sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        final ProfilingInfo successful = start();
        awaitCompletion();
        sampler("exit 1");
        start();
        awaitCompletion();
        start();
        awaitCompletion();
        assertThat(output.resolve(successful.getOutputFile())).isRegularFile();
        assertThat(service.getProfilingList("tm").get()).hasSize(2).contains(successful);
    }

    @Test
    void retainsSuccessfulResultsForDifferentWorkers() throws Exception {
        sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        final ProfilingInfo first = start();
        awaitCompletion();
        final Process secondProcess = new ProcessBuilder("/bin/sh", "-c", "exec sleep 60").start();
        try {
            final Worker second = registerWorker(secondProcess);
            ProfilingInfo latest = null;
            for (int i = 0; i < 2; i++) {
                latest =
                        service.requestPythonProfiling(
                                        "tm", 2, second, registry, sampler.toString(), false, false)
                                .get();
                awaitCompletion();
            }
            assertThat(service.getProfilingList("tm").get()).hasSize(2).contains(first, latest);
            assertThat(output.resolve(first.getOutputFile())).isRegularFile();
        } finally {
            secondProcess.destroyForcibly();
            assertThat(secondProcess.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void startupCaptureSharesTheJvmAndCpuSlot() throws Exception {
        try (FileChannel channel =
                        FileChannel.open(
                                service.getProfilingLockFile(),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE);
                FileLock lock = channel.lock()) {
            assertThat(lock.isValid()).isTrue();
            assertThatThrownBy(() -> start()).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(
                            () ->
                                    service.requestProfiling(
                                                    "tm", 1, ProfilingInfo.ProfilingMode.ITIMER)
                                            .get())
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
        sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        assertThat(start().getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.RUNNING);
        awaitCompletion();
    }

    @Test
    void pythonResultsDoNotEvictJvmHistory() throws Exception {
        Files.createDirectories(output);
        final ProfilingInfo jvm =
                ProfilingInfo.create(1, ProfilingInfo.ProfilingMode.ITIMER).success("jvm.html");
        Files.writeString(output.resolve(jvm.getOutputFile()), "JVM result fixture");
        service.getProfilingListForTest("tm").add(jvm);
        sampler("printf '<svg>TEST FIXTURE</svg>' > \"$output\"");
        for (int i = 0; i < 3; i++) {
            start();
            awaitCompletion();
        }
        assertThat(service.getProfilingList("tm").get()).hasSize(2).contains(jvm);
        assertThat(output.resolve(jvm.getOutputFile())).isRegularFile();
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
                        + "'\ntrap '' INT\nexec sleep 60");
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
    void stopsSamplingAtDurationAndWaitsForOutput() throws Exception {
        final Path stopped = temporary.resolve("stopped");
        final Path release = temporary.resolve("finish-output");
        sampler(
                "trap 'echo stopped > \""
                        + stopped
                        + "\"; while [ ! -e \""
                        + release
                        + "\" ]; do sleep 0.05; done; "
                        + "printf \"<svg>FINISHED AFTER INTERRUPT</svg>\" > \"$output\"; "
                        + "echo \"Samples: 1 Errors: 0\"; exit 0' INT\n"
                        + "while :; do sleep 0.05; done");
        final ProfilingInfo info = start();
        try {
            waitUntilCondition(() -> Files.exists(stopped), 20L, 250);
            assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.RUNNING);
            assertThat(info.getOutputFile()).isNull();
        } finally {
            Files.writeString(release, "finish");
        }
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
        assertThat(output.resolve(info.getOutputFile()))
                .hasContent("<svg>FINISHED AFTER INTERRUPT</svg>");
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

    private void memorySampler(boolean failDetach) throws Exception {
        Files.writeString(
                sampler,
                "#!/bin/sh\nif [ \"$1\" = detach ]; then exit "
                        + (failDetach ? "1" : "0")
                        + "; fi\n"
                        + "while [ \"$#\" -gt 0 ]; do if [ \"$1\" = -o ]; then shift; output=$1; fi; shift; done\n"
                        + "printf 'MEMRAY TEST FIXTURE' > \"$output\"\n");
        Files.setPosixFilePermissions(sampler, PosixFilePermissions.fromString("rwx------"));
    }

    @Test
    void memoryCapturePublishesOnlyAfterDetach() throws Exception {
        memorySampler(false);
        final ProfilingInfo info =
                service.requestPythonMemoryProfiling(
                                "tm", 1, worker, registry, sampler.toString(), true)
                        .get();
        assertThat(info.getType()).isEqualTo(ProfilingInfo.PythonProfilingType.MEMORY);
        assertThat(info.getOutputFile()).isNull();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FINISHED);
        assertThat(output.resolve(info.getOutputFile())).hasContent("MEMRAY TEST FIXTURE");
        assertThat(info.getIncludeIdle()).isNull();
        assertThat(workerProcess.isAlive()).isTrue();
    }

    @Test
    void failedMemoryDetachRetainsReservationAndIncompleteFile() throws Exception {
        memorySampler(true);
        final ProfilingInfo info =
                service.requestPythonMemoryProfiling(
                                "tm", 1, worker, registry, sampler.toString(), false)
                        .get();
        waitUntilCondition(
                () -> info.getStatus() == ProfilingInfo.ProfilingStatus.FAILED, 20L, 500);
        assertThat(info.getMessage()).contains("stop unconfirmed");
        assertThat(info.getOutputFile()).isNull();
        assertThat(service.getProfilingFuture().isDone()).isFalse();
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            assertThat(files.filter(path -> path.toString().endsWith(".inprogress"))).hasSize(1);
        }
        workerProcess.destroyForcibly();
        workerProcess.waitFor();
        awaitCompletion();
    }

    @Test
    void cancelledMemoryAttachReapsDebuggerBeforeDetach() throws Exception {
        final Path debuggerPid = temporary.resolve("debugger-pid");
        Files.writeString(
                sampler,
                "#!/bin/sh\nif [ \"$1\" = detach ]; then exit 0; fi\n"
                        + "sleep 60 &\nchild=$!\necho $child > '"
                        + debuggerPid
                        + "'\nwait $child\n");
        Files.setPosixFilePermissions(sampler, PosixFilePermissions.fromString("rwx------"));
        final ProfilingInfo info =
                service.requestPythonMemoryProfiling(
                                "tm", 30, worker, registry, sampler.toString(), false)
                        .get();
        waitUntilCondition(() -> Files.exists(debuggerPid) && Files.size(debuggerPid) > 0);
        final long pid = Long.parseLong(Files.readString(debuggerPid).trim());
        service.cancelPythonProfiling("tm").get(10, TimeUnit.SECONDS);
        assertThat(ProcessHandle.of(pid).filter(ProcessHandle::isAlive)).isEmpty();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).doesNotContain("stop unconfirmed");
        assertThat(workerProcess.isAlive()).isTrue();
    }

    @Test
    void missingMemoryExecutableDoesNotReserveWorkerUntilExit() throws Exception {
        final ProfilingInfo info =
                service.requestPythonMemoryProfiling(
                                "tm", 1, worker, registry, sampler.toString(), false)
                        .get();
        awaitCompletion();
        assertThat(info.getStatus()).isEqualTo(ProfilingInfo.ProfilingStatus.FAILED);
        assertThat(info.getMessage()).doesNotContain("stop unconfirmed");
        assertThat(workerProcess.isAlive()).isTrue();
    }

    @Test
    void removesDeadRegistrationButPreservesUnrecognizedFiles() throws Exception {
        final Path directory = PythonWorkerRegistry.getRegistrationDirectory(temporary.toFile());
        final Path unrelated = directory.resolve("keep.txt");
        Files.writeString(unrelated, "keep");
        workerProcess.destroyForcibly();
        workerProcess.waitFor();
        assertThat(registry.listWorkers()).isEmpty();
        assertThat(directory.resolve(worker.getId() + ".json")).doesNotExist();
        assertThat(unrelated).hasContent("keep");
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
