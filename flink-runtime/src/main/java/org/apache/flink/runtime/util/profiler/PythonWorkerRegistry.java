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

import org.apache.flink.runtime.rest.messages.PythonWorkerInfo;
import org.apache.flink.runtime.rest.messages.PythonWorkerInfo.Operator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Reads standalone-worker registrations from this TaskManager's private working directory. */
public class PythonWorkerRegistry {
    public static final String ENVIRONMENT_VARIABLE = "PYFLINK_WORKER_PROFILE_DIR";
    public static final String JOB_ID_ENVIRONMENT_VARIABLE = "PYFLINK_WORKER_PROFILE_JOB_ID";
    private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerRegistry.class);
    private final Path directory;
    private final ObjectMapper mapper = new ObjectMapper();

    public PythonWorkerRegistry(File workingDirectory) {
        directory = getRegistrationDirectory(workingDirectory);
    }

    public static Path getRegistrationDirectory(File workingDirectory) {
        return workingDirectory.toPath().resolve("python-workers");
    }

    public Path registerOperator(String workerId, String executionId, Operator operator)
            throws IOException {
        final Path operators = directory.resolve("operators");
        Files.createDirectories(operators);
        final Path registration =
                operators.resolve(executionId + "_" + operator.getOperatorId() + ".json");
        final Path temporary = registration.resolveSibling(registration.getFileName() + ".tmp");
        final ObjectNode record = mapper.createObjectNode();
        record.put("worker_id", workerId);
        record.set("operator", mapper.valueToTree(operator));
        try {
            mapper.writeValue(temporary.toFile(), record);
            Files.move(temporary, registration, StandardCopyOption.ATOMIC_MOVE);
            return registration;
        } finally {
            IOUtils.deleteFileQuietly(temporary);
        }
    }

    private Map<String, List<Operator>> readOperators() throws IOException {
        final Map<String, List<Operator>> result = new HashMap<>();
        final Path operators = directory.resolve("operators");
        if (Files.notExists(operators)) {
            return result;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(operators, "*.json")) {
            for (Path entry : entries) {
                if (!Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                try {
                    final JsonNode record = mapper.readTree(Files.readString(entry));
                    if (!record.path("operator").isObject()) {
                        continue;
                    }
                    final Operator operator =
                            mapper.treeToValue(record.get("operator"), Operator.class);
                    result.computeIfAbsent(
                                    operator.getJobId() + ":" + record.path("worker_id").asText(),
                                    ignored -> new ArrayList<>())
                            .add(operator);
                } catch (NoSuchFileException e) {
                    LOG.debug("Python operator registration was removed: {}", entry);
                } catch (IOException e) {
                    LOG.warn("Could not read Python operator registration {}", entry, e);
                }
            }
        }
        return result;
    }

    public Collection<Worker> listWorkers() throws IOException {
        if (Files.notExists(directory)) {
            return Collections.emptyList();
        }
        final Collection<Worker> workers = new ArrayList<>();
        final Map<String, List<Operator>> operators = readOperators();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*.json")) {
            for (Path entry : entries) {
                if (!Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                try {
                    final JsonNode registration = mapper.readTree(Files.readString(entry));
                    final String id = registration.path("registration_id").asText();
                    if (!id.matches("[0-9a-f]{32}")
                            || !entry.getFileName().toString().equals(id + ".json")) {
                        continue;
                    }
                    final Worker worker =
                            new Worker(
                                    id,
                                    registration.path("pid").asLong(-1),
                                    registration.path("start_token").asText(),
                                    registration.path("memory_available").asBoolean(false),
                                    registration.path("memory_busy").asBoolean(false),
                                    operators.getOrDefault(
                                            registration.path("job_id").asText()
                                                    + ":"
                                                    + registration.path("worker_id").asText(),
                                            Collections.emptyList()));
                    if (worker.getPid() <= 0 || worker.getStartToken().isEmpty()) {
                        continue;
                    }
                    if (isAlive(worker)) {
                        workers.add(worker);
                    } else {
                        try {
                            Files.deleteIfExists(entry);
                        } catch (IOException e) {
                            LOG.warn(
                                    "Could not remove stale Python worker registration {}",
                                    entry,
                                    e);
                        }
                    }
                } catch (NoSuchFileException e) {
                    // Atomic registration can still disappear during normal worker shutdown.
                    LOG.debug("Could not read Python worker registration {}", entry, e);
                } catch (IOException e) {
                    LOG.warn("Could not inspect Python worker registration {}", entry, e);
                }
            }
        }
        return workers;
    }

    public Worker getWorker(String id) throws IOException {
        return listWorkers().stream()
                .filter(worker -> worker.getId().equals(id))
                .findFirst()
                .orElseThrow(
                        () ->
                                new NoSuchElementException(
                                        "Python worker is absent or has exited: " + id));
    }

    public boolean isAlive(Worker worker) throws IOException {
        if (worker.getPid() <= 0 || worker.getStartToken().isEmpty()) {
            return false;
        }
        final Optional<ProcessHandle> handle = ProcessHandle.of(worker.getPid());
        if (handle.isEmpty() || !handle.get().isAlive()) {
            return false;
        }
        Optional<ProcessHandle> parent = handle.get().parent();
        while (parent.isPresent()) {
            if (parent.get().pid() == ProcessHandle.current().pid()) {
                return worker.getStartToken().equals(readStartToken(worker.getPid()));
            }
            parent = parent.get().parent();
        }
        return false;
    }

    static String readStartToken(long pid) throws IOException {
        if (OperatingSystem.isLinux()) {
            final Path stat = Path.of("/proc", Long.toString(pid), "stat");
            if (!Files.exists(stat)) {
                return "";
            }
            final String value = Files.readString(stat);
            final String[] fields = value.substring(value.lastIndexOf(')') + 2).split(" ");
            if (fields[0].equals("Z")) {
                return "";
            }
            return Files.readString(Path.of("/proc/sys/kernel/random/boot_id")).trim()
                    + ":"
                    + fields[19];
        }
        if (OperatingSystem.isMac()) {
            final ProcessBuilder builder =
                    new ProcessBuilder(
                            "/bin/ps", "-p", Long.toString(pid), "-o", "lstart=", "-o", "stat=");
            builder.environment().put("LC_ALL", "C");
            final Process process = builder.start();
            try {
                if (!process.waitFor(2, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out reading Python worker start time");
                }
                final String value =
                        new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                                .trim();
                if (process.exitValue() != 0 || value.isEmpty()) {
                    return "";
                }
                final String[] fields = value.split("\\s+");
                if (fields[fields.length - 1].startsWith("Z")) {
                    return "";
                }
                return String.join(" ", Arrays.copyOf(fields, fields.length - 1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted reading worker identity", e);
            } finally {
                process.destroy();
            }
        }
        throw new IOException("Python profiling supports Linux and macOS only");
    }

    /** Internal process identity; only its public descriptor crosses the REST boundary. */
    public static final class Worker {
        private final String id;
        private final long pid;
        private final String startToken;
        private final boolean memoryAvailable;
        private final boolean memoryBusy;
        private final List<Operator> operators;

        public boolean isMemoryAvailable() {
            return memoryAvailable;
        }

        public boolean isMemoryBusy() {
            return memoryBusy;
        }

        Worker(String id, long pid, String startToken) {
            this(id, pid, startToken, false, false, Collections.emptyList());
        }

        Worker(
                String id,
                long pid,
                String startToken,
                boolean memoryAvailable,
                boolean memoryBusy,
                List<Operator> operators) {
            this.operators = List.copyOf(operators);
            this.memoryAvailable = memoryAvailable;
            this.memoryBusy = memoryBusy;
            this.id = id;
            this.pid = pid;
            this.startToken = startToken;
        }

        String getId() {
            return id;
        }

        long getPid() {
            return pid;
        }

        String getStartToken() {
            return startToken;
        }

        public PythonWorkerInfo toInfo() {
            return new PythonWorkerInfo(id, pid, memoryAvailable, operators);
        }
    }
}
