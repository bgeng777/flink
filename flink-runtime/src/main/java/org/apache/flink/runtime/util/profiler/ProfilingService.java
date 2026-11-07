/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.util.profiler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.util.profiler.PythonWorkerRegistry.Worker;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Create and keep profiling requests with rolling policy. */
public class ProfilingService implements Closeable {

    protected static final Logger LOG = LoggerFactory.getLogger(ProfilingService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
    private static volatile ProfilingService instance;
    private final Map<String, ArrayDeque<ProfilingInfo>> profilingMap;
    private final String profilingResultDir;
    private final int historySizeLimit;
    private final ScheduledExecutorService scheduledExecutor;
    private ProfilingFuture profilingFuture;
    @Nullable private String pythonProfilingResourceID;
    private CompletableFuture<Void> pythonProfilingCompletion = FutureUtils.completedVoidFuture();

    private ProfilingService(Configuration configs) {
        this.profilingMap = new HashMap<>();
        this.historySizeLimit = configs.get(RestOptions.MAX_PROFILING_HISTORY_SIZE);
        Preconditions.checkArgument(
                historySizeLimit > 0,
                String.format(
                        "Configured %s must be positive.",
                        RestOptions.MAX_PROFILING_HISTORY_SIZE.key()));
        this.profilingResultDir = configs.get(RestOptions.PROFILING_RESULT_DIR);
        this.scheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory.Builder()
                                .setPoolName("flink-profiling-service")
                                .build());
    }

    public static ProfilingService getInstance(Configuration configs) {
        if (instance == null) {
            synchronized (ProfilingService.class) {
                if (instance == null) {
                    instance = new ProfilingService(configs);
                }
            }
        }
        return instance;
    }

    public synchronized CompletableFuture<ProfilingInfo> requestProfiling(
            String resourceID, long duration, ProfilingInfo.ProfilingMode mode) {
        if (profilingFuture != null && !profilingFuture.isDone()) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(resourceID + " is still under profiling."));
        }
        ProfilingInfo profilingInfo = ProfilingInfo.create(duration, mode);
        profilingMap.putIfAbsent(resourceID, new ArrayDeque<>());
        profilingMap.get(resourceID).addFirst(profilingInfo);
        rollingClearing(profilingMap.get(resourceID));
        AsyncProfiler profiler = AsyncProfiler.getInstance();
        try {
            String response =
                    profiler.execute(
                            ProfilerConstants.COMMAND_START.msg
                                    + profilingInfo.getProfilingMode().getCode());
            if (StringUtils.isNullOrWhitespaceOnly(response)
                    || !response.startsWith(ProfilerConstants.PROFILER_STARTED_SUCCESS.msg)) {
                return CompletableFuture.completedFuture(
                        profilingInfo.fail("Start profiler failed. " + response));
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                    profilingInfo.fail("Start profiler failed. " + e));
        }

        this.profilingFuture = new ProfilingFuture(duration, () -> stopProfiling(resourceID));
        return CompletableFuture.completedFuture(profilingInfo);
    }

    public synchronized CompletableFuture<ProfilingInfo> requestPythonProfiling(
            String resourceID,
            long duration,
            Worker worker,
            PythonWorkerRegistry registry,
            String executable,
            boolean includeIdle,
            boolean includeNative) {
        if (profilingFuture != null && !profilingFuture.isDone()) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(resourceID + " is still under profiling."));
        }
        final ProfilingInfo info =
                ProfilingInfo.createPython(duration, worker.toInfo(), includeIdle, includeNative);
        profilingMap.putIfAbsent(resourceID, new ArrayDeque<>());
        profilingMap.get(resourceID).addFirst(info);
        rollingClearing(profilingMap.get(resourceID));
        final PythonWorkerProfiler profiler =
                new PythonWorkerProfiler(
                        registry,
                        worker,
                        executable,
                        Paths.get(profilingResultDir),
                        duration,
                        includeIdle,
                        includeNative);
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        pythonProfilingResourceID = resourceID;
        pythonProfilingCompletion = completion;
        profilingFuture = new ProfilingFuture(completion, profiler::cancel);
        scheduledExecutor.execute(
                () -> {
                    try {
                        final String filename = profiler.profile();
                        synchronized (this) {
                            info.success(filename);
                        }
                    } catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        synchronized (this) {
                            info.fail("Python profiling failed: " + e.getMessage());
                        }
                    } finally {
                        completion.complete(null);
                    }
                });
        return CompletableFuture.completedFuture(info);
    }

    public synchronized CompletableFuture<ProfilingInfo> requestPythonMemoryProfiling(
            String resourceID,
            long duration,
            Worker worker,
            PythonWorkerRegistry registry,
            String executable,
            boolean includeNative) {
        if (profilingFuture != null && !profilingFuture.isDone()) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException("A profiling session is active in this Flink JVM"));
        }
        final ProfilingInfo info =
                ProfilingInfo.createPython(duration, worker.toInfo(), false, includeNative);
        info.setType(ProfilingInfo.PythonProfilingType.MEMORY);
        profilingMap.computeIfAbsent(resourceID, ignored -> new ArrayDeque<>()).addFirst(info);
        rollingClearing(profilingMap.get(resourceID));
        final Optional<ProcessHandle> workerHandle = ProcessHandle.of(worker.getPid());
        final PythonWorkerMemoryProfiler profiler =
                new PythonWorkerMemoryProfiler(
                        registry,
                        worker,
                        executable,
                        Paths.get(profilingResultDir),
                        duration,
                        includeNative);
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        pythonProfilingResourceID = resourceID;
        pythonProfilingCompletion = completion;
        profilingFuture = new ProfilingFuture(completion, profiler::cancel);
        scheduledExecutor.execute(
                () -> {
                    try {
                        final String file = profiler.profile();
                        synchronized (this) {
                            info.success(file);
                        }
                    } catch (Exception error) {
                        if (error instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        synchronized (this) {
                            info.fail(
                                    "Python memory profiling failed: "
                                            + error.getMessage()
                                            + (profiler.isStopConfirmed()
                                                    ? ""
                                                    : "; stop unconfirmed; profiling remains reserved until the worker exits"));
                        }
                    } finally {
                        if (profiler.isStopConfirmed()) {
                            completion.complete(null);
                        } else {
                            workerHandle.ifPresentOrElse(
                                    handle ->
                                            handle.onExit()
                                                    .whenComplete(
                                                            (ignored, failure) ->
                                                                    completion.complete(null)),
                                    () -> completion.complete(null));
                        }
                    }
                });
        return CompletableFuture.completedFuture(info);
    }

    /** Cancels this TaskManager's Python capture with a bounded wait for sampler cleanup. */
    public synchronized CompletableFuture<Void> cancelPythonProfiling(String resourceID) {
        if (resourceID.equals(pythonProfilingResourceID) && !pythonProfilingCompletion.isDone()) {
            profilingFuture.cancel();
            return pythonProfilingCompletion
                    .thenApply(ignored -> (Void) null)
                    .completeOnTimeout(null, 15, TimeUnit.SECONDS);
        }
        return FutureUtils.completedVoidFuture();
    }

    private synchronized void stopProfiling(String resourceID) {
        AsyncProfiler profiler = AsyncProfiler.getInstance();
        ArrayDeque<ProfilingInfo> profilingList = profilingMap.get(resourceID);
        Preconditions.checkState(!CollectionUtil.isNullOrEmpty(profilingList));
        ProfilingInfo info = profilingList.getFirst();
        try {
            String fileName = formatOutputFileName(resourceID, info);
            String outputPath = new File(profilingResultDir, fileName).getPath();
            String response = profiler.execute(ProfilerConstants.COMMAND_STOP.msg + outputPath);
            if (!StringUtils.isNullOrWhitespaceOnly(response)
                    && response.startsWith(ProfilerConstants.PROFILER_STOPPED_SUCCESS.msg)) {
                info.success(fileName);
            } else {
                info.fail("Stop profiler failed. " + response);
            }
            rollingClearing(profilingList);
        } catch (Throwable e) {
            info.fail("Stop profiler failed. " + e);
        }
    }

    private void rollingClearing(ArrayDeque<ProfilingInfo> profilingList) {
        while (profilingList.size() > historySizeLimit) {
            ProfilingInfo info = profilingList.pollLast();
            String outputFile = info != null ? info.getOutputFile() : "";
            if (StringUtils.isNullOrWhitespaceOnly(outputFile)) {
                continue;
            }
            try {
                Files.deleteIfExists(Paths.get(profilingResultDir, outputFile));
            } catch (Exception e) {
                LOG.error(String.format("Clearing file for %s failed. Skipped.", info), e);
            }
        }
    }

    private String formatOutputFileName(String resourceID, ProfilingInfo info) {
        return String.format(
                "%s_%s_%s.html", resourceID, info.getProfilingMode(), sdf.format(new Date()));
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (profilingFuture != null && !profilingFuture.isDone()) {
                profilingFuture.cancel();
            }
            if (!scheduledExecutor.isShutdown()) {
                scheduledExecutor.shutdown();
            }
        } catch (Exception e) {
            LOG.error("Exception thrown during stopping profiling service. ", e);
        } finally {
            instance = null;
        }
    }

    public synchronized CompletableFuture<Collection<ProfilingInfo>> getProfilingList(
            String resourceID) {
        return CompletableFuture.completedFuture(
                new ArrayList<>(profilingMap.getOrDefault(resourceID, new ArrayDeque<>())));
    }

    public String getProfilingResultDir() {
        return profilingResultDir;
    }

    @VisibleForTesting
    ArrayDeque<ProfilingInfo> getProfilingListForTest(String resourceID) {
        return profilingMap.getOrDefault(resourceID, new ArrayDeque<>());
    }

    @VisibleForTesting
    int getHistorySizeLimit() {
        return historySizeLimit;
    }

    @VisibleForTesting
    ProfilingFuture getProfilingFuture() {
        return profilingFuture;
    }

    enum ProfilerConstants {
        PROFILER_STARTED_SUCCESS("Profiling started"),
        PROFILER_STOPPED_SUCCESS("OK"),
        COMMAND_START("start,event="),
        COMMAND_STOP("stop,file=");

        private final String msg;

        ProfilerConstants(String msg) {
            this.msg = msg;
        }
    }

    /**
     * ProfilingFuture takes a handler and trigger duration, which will call the handler when
     * timeout or cancelled.
     */
    class ProfilingFuture {
        private final Future<?> future;
        private final BooleanSupplier cancelAction;

        public ProfilingFuture(long duration, Runnable handler) {
            final ScheduledFuture<?> scheduled =
                    scheduledExecutor.schedule(handler, duration, TimeUnit.SECONDS);
            this.future = scheduled;
            this.cancelAction =
                    () -> {
                        if (!scheduled.cancel(true)) {
                            return false;
                        }
                        handler.run();
                        return true;
                    };
        }

        ProfilingFuture(CompletableFuture<?> completion, Runnable cancel) {
            this.future = completion;
            this.cancelAction =
                    () -> {
                        cancel.run();
                        return true;
                    };
        }

        /**
         * Verify the profiling request was done or not.
         *
         * @return true represents the request was finished.
         */
        public boolean isDone() {
            return this.future == null || this.future.isDone();
        }

        /**
         * Try to cancel the Profiling Request. Note that we'll trigger the handler once cancelled
         * successfully.
         *
         * @return true if cancelled, false for failed.
         */
        public boolean cancel() {
            if (isDone()) {
                return true;
            }
            return cancelAction.getAsBoolean();
        }
    }
}
