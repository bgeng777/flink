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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs an opt-in Memray capture. CLI completion alone is not Tracker completion. */
class PythonWorkerMemoryProfiler {
    private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerMemoryProfiler.class);
    private static final long MAX_RECORDING_BYTES = 256L * 1024 * 1024;
    private final PythonWorkerRegistry registry;
    private final Worker worker;
    private final String executable;
    private final Path directory;
    private final long duration;
    private final boolean nativeTraces;
    private final CountDownLatch cancelled = new CountDownLatch(1);
    private boolean stopConfirmed = true;
    private boolean detachAttempted;
    private boolean toolsReaped = true;

    PythonWorkerMemoryProfiler(
            PythonWorkerRegistry registry,
            Worker worker,
            String executable,
            Path directory,
            long duration,
            boolean nativeTraces) {
        this.registry = registry;
        this.worker = worker;
        this.executable = executable;
        this.directory = directory;
        this.duration = duration;
        this.nativeTraces = nativeTraces;
    }

    void cancel() {
        cancelled.countDown();
    }

    boolean isStopConfirmed() {
        return stopConfirmed;
    }

    String profile() throws IOException, InterruptedException {
        final String name = "python-memory-" + UUID.randomUUID() + ".bin";
        final Path pending = directory.resolve(name + ".inprogress");
        final Path log = directory.resolve(name + ".log");
        boolean success = false;
        try {
            if (!Path.of(executable).isAbsolute()) {
                throw new IOException("Memray executable must be an absolute path");
            }
            checkWorker();
            Files.createDirectories(directory);
            final List<String> command =
                    new ArrayList<>(
                            Arrays.asList(
                                    executable,
                                    "attach",
                                    Long.toString(worker.getPid()),
                                    "-o",
                                    pending.toString(),
                                    "--duration",
                                    Long.toString(duration + 15)));
            if (nativeTraces) {
                command.add("--native");
            }
            // From this point, an interrupted CLI may already have installed a Tracker.
            stopConfirmed = false;
            runCommand(command, log, true);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(duration);
            while (System.nanoTime() < deadline) {
                checkWorker();
                if (Files.exists(pending) && Files.size(pending) > MAX_RECORDING_BYTES) {
                    throw new IOException("Memray recording exceeded its size limit");
                }
                cancelled.await(100, TimeUnit.MILLISECONDS);
            }
            checkWorker();
            detach(log);
            if (!Files.isRegularFile(pending) || Files.size(pending) == 0) {
                throw new IOException("Memray produced no recording");
            }
            checkWorker();
            Files.move(pending, directory.resolve(name));
            success = true;
            return name;
        } finally {
            if (!stopConfirmed && !detachAttempted && toolsReaped) {
                try {
                    if (!registry.isAlive(worker)) {
                        stopConfirmed = true;
                    } else {
                        detach(log);
                    }
                } catch (Exception failure) {
                    if (failure instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    LOG.warn(
                            "Memray stop unconfirmed for worker {}; retaining incomplete recording {}",
                            worker.getId(),
                            pending,
                            failure);
                }
            }
            if (stopConfirmed) {
                if (!success) {
                    delete(pending);
                }
                delete(log);
            }
        }
    }

    private void detach(Path log) throws IOException, InterruptedException {
        detachAttempted = true;
        if (!registry.isAlive(worker)) {
            stopConfirmed = true;
            throw new IOException("Python worker exited or changed identity");
        }
        runCommand(Arrays.asList(executable, "detach", Long.toString(worker.getPid())), log, false);
        stopConfirmed = true;
    }

    private void checkWorker() throws IOException {
        if (cancelled.getCount() == 0) {
            throw new IOException("Python memory profiling was cancelled");
        }
        if (!registry.isAlive(worker)) {
            throw new IOException("Python worker exited or changed identity");
        }
    }

    private void runCommand(List<String> command, Path log, boolean attaching)
            throws IOException, InterruptedException {
        final Process process;
        try {
            process =
                    new ProcessBuilder(command)
                            .redirectErrorStream(true)
                            .redirectOutput(log.toFile())
                            .start();
        } catch (IOException error) {
            if (attaching) {
                stopConfirmed = true;
            }
            throw error;
        }
        final Set<ProcessHandle> children = new HashSet<>();
        try {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                process.descendants().forEach(children::add);
                if (process.waitFor(50, TimeUnit.MILLISECONDS)) {
                    break;
                }
                if (attaching) {
                    checkWorker();
                }
                if (System.nanoTime() >= deadline || Files.size(log) > 1024 * 1024) {
                    throw new IOException("Memray command exceeded its time or output limit");
                }
            }
            if (process.exitValue() != 0) {
                try (InputStream input = Files.newInputStream(log)) {
                    throw new IOException(
                            "Memray "
                                    + command.get(1)
                                    + " failed: "
                                    + new String(input.readNBytes(4096), StandardCharsets.UTF_8));
                }
            }
        } finally {
            process.descendants().forEach(children::add);
            for (ProcessHandle child : children) {
                child.destroyForcibly();
            }
            try {
                for (ProcessHandle child : children) {
                    try {
                        child.onExit().get(2, TimeUnit.SECONDS);
                    } catch (ExecutionException | TimeoutException e) {
                        toolsReaped = false;
                        LOG.warn("Could not confirm debugger process {} exited", child.pid(), e);
                    } catch (InterruptedException e) {
                        toolsReaped = false;
                        throw e;
                    }
                }
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    if (!process.waitFor(2, TimeUnit.SECONDS)) {
                        toolsReaped = false;
                    }
                }
            }
            if (!toolsReaped) {
                throw new IOException("Debugger termination unconfirmed");
            }
        }
    }

    private void delete(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.warn("Could not remove memory profiling artifact {}", file, e);
        }
    }
}
