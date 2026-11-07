/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.resourcemanager.AbstractResourceManagerHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.PythonWorkerInfo;
import org.apache.flink.runtime.rest.messages.PythonWorkerInfoList;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Lists standalone Python workers on a TaskManager. */
public class TaskManagerPythonWorkerListHandler
        extends AbstractResourceManagerHandler<
                RestfulGateway,
                EmptyRequestBody,
                PythonWorkerInfoList,
                TaskManagerMessageParameters> {

    public TaskManagerPythonWorkerListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, PythonWorkerInfoList, TaskManagerMessageParameters>
                    messageHeaders,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                resourceManagerGatewayRetriever);
    }

    @Override
    protected CompletableFuture<PythonWorkerInfoList> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull ResourceManagerGateway gateway)
            throws RestHandlerException {
        final ResourceID taskManagerId = request.getPathParameter(TaskManagerIdPathParameter.class);
        final CompletableFuture<Collection<PythonWorkerInfo>> workersFuture =
                gateway.requestPythonWorkers(taskManagerId, getTimeout());

        return workersFuture
                .thenApply(PythonWorkerInfoList::new)
                .exceptionally(
                        (throwable) -> {
                            final Throwable strippedThrowable =
                                    ExceptionUtils.stripCompletionException(throwable);
                            if (strippedThrowable instanceof UnsupportedOperationException) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                strippedThrowable.getMessage(),
                                                HttpResponseStatus.SERVICE_UNAVAILABLE,
                                                strippedThrowable));
                            }
                            if (strippedThrowable instanceof UnknownTaskExecutorException) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Could not find TaskExecutor " + taskManagerId,
                                                HttpResponseStatus.NOT_FOUND,
                                                strippedThrowable));
                            } else {
                                throw new CompletionException(throwable);
                            }
                        });
    }
}
