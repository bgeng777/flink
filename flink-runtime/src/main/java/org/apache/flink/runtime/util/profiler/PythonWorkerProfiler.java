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

import org.apache.flink.runtime.util.profiler.PythonWorkerRegistry.Worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Executes one bounded py-spy capture. Session history belongs to {@link ProfilingService}. */
class PythonWorkerProfiler {
    private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerProfiler.class);
    private static final long MAX_OUTPUT_BYTES = 20 * 1024 * 1024;
    private static final Pattern SAMPLES = Pattern.compile("\\bSamples: [1-9][0-9]*\\b");
    private static final long MAX_LOG_BYTES = 1024 * 1024;
    private static final long FINALIZATION_TIMEOUT_SECONDS = 10;
    private final PythonWorkerRegistry registry;
    private final Worker worker;
    private final String executable;
    private final Path outputDirectory;
    private final long duration;
    private final boolean includeIdle;
    private final boolean includeNative;
    private final AtomicBoolean cancelled = new AtomicBoolean();

    PythonWorkerProfiler(
            PythonWorkerRegistry registry,
            Worker worker,
            String executable,
            Path outputDirectory,
            long duration,
            boolean includeIdle,
            boolean includeNative) {
        this.registry = registry;
        this.worker = worker;
        this.executable = executable;
        this.outputDirectory = outputDirectory;
        this.duration = duration;
        this.includeIdle = includeIdle;
        this.includeNative = includeNative;
    }

    void cancel() {
        cancelled.set(true);
    }

    String profile() throws IOException, InterruptedException {
        final String filename = "python-" + UUID.randomUUID() + ".svg";
        final Path output = outputDirectory.resolve(filename);
        final Path log = outputDirectory.resolve(filename + ".log");
        final List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                executable,
                                "record",
                                "--pid",
                                Long.toString(worker.getPid()),
                                "--duration",
                                Long.toString(duration),
                                "--rate",
                                "50",
                                "--threads",
                                "--format",
                                "flamegraph",
                                "--output",
                                output.toString()));
        if (includeIdle) {
            command.add("--idle");
        }
        if (includeNative) {
            command.add("--native");
        }
        Process process = null;
        boolean success = false;
        try {
            checkWorker();
            if (!Path.of(executable).isAbsolute()) {
                throw new IOException("The py-spy executable must be an absolute path");
            }
            Files.createDirectories(outputDirectory);
            process =
                    new ProcessBuilder(command)
                            .redirectErrorStream(true)
                            .redirectOutput(log.toFile())
                            .start();
            // py-spy's duration counts sampling intervals, which can overrun with slow unwinding.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(duration);
            boolean stopRequested = false;
            while (!process.waitFor(100, TimeUnit.MILLISECONDS)) {
                checkWorker();
                if (System.nanoTime() >= deadline) {
                    if (stopRequested) {
                        throw new IOException("Python sampler exceeded its finalization deadline");
                    }
                    stopSampling(process);
                    stopRequested = true;
                    deadline =
                            System.nanoTime()
                                    + TimeUnit.SECONDS.toNanos(FINALIZATION_TIMEOUT_SECONDS);
                }
                checkOutputSize(output, log);
            }
            checkWorker();
            checkOutputSize(output, log);
            if (process.exitValue() != 0) {
                try (InputStream errors = Files.newInputStream(log)) {
                    throw new IOException(
                            "py-spy exited with code "
                                    + process.exitValue()
                                    + ": "
                                    + new String(errors.readNBytes(4096), StandardCharsets.UTF_8));
                }
            }
            if (!Files.isRegularFile(output) || Files.size(output) == 0) {
                throw new IOException("Python sampler produced no flamegraph");
            }
            // py-spy may finish successfully without collecting usable samples.
            try (Stream<String> lines = Files.lines(log)) {
                if (lines.noneMatch(line -> SAMPLES.matcher(line).find())) {
                    throw new IOException("py-spy did not report any collected samples");
                }
            }
            success = true;
            return filename;
        } finally {
            try {
                if (process != null && process.isAlive()) {
                    process.destroy();
                    if (!process.waitFor(1, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor(1, TimeUnit.SECONDS);
                    }
                }
            } finally {
                if (!success) {
                    deleteResult(output);
                }
                deleteResult(log);
            }
        }
    }

    private void stopSampling(Process process) throws IOException, InterruptedException {
        if (!process.isAlive()) {
            return;
        }
        final Process signal =
                new ProcessBuilder("/bin/kill", "-INT", Long.toString(process.pid()))
                        .redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                        .start();
        try {
            if (!signal.waitFor(1, TimeUnit.SECONDS)
                    || (signal.exitValue() != 0 && process.isAlive())) {
                throw new IOException("Could not interrupt Python sampler");
            }
        } finally {
            if (signal.isAlive()) {
                signal.destroyForcibly();
                signal.waitFor(1, TimeUnit.SECONDS);
            }
        }
    }

    private void checkWorker() throws IOException {
        if (cancelled.get()) {
            throw new IOException("Python profiling was cancelled");
        }
        if (!registry.isAlive(worker)) {
            throw new IOException("Python worker exited or changed identity");
        }
    }

    private void checkOutputSize(Path output, Path log) throws IOException {
        if (Files.size(log) > MAX_LOG_BYTES
                || (Files.exists(output) && Files.size(output) > MAX_OUTPUT_BYTES)) {
            throw new IOException("Python sampler output exceeded its size limit");
        }
    }

    private void deleteResult(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.warn("Could not remove Python profiling artifact {}", path, e);
        }
    }
}
