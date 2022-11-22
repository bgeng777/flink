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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.operator.CepOperator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.cep.SimplePythonCondition;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import pemja.core.object.PyIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/**
 * {@link EmbeddedPythonCepOperator} is responsible for executing Python Cep in embedded Python
 * environment.
 */
@Internal
public class EmbeddedPythonCepOperator<K, IN, OUT>
        extends AbstractOneInputEmbeddedPythonFunctionOperator<IN, OUT>
        implements Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    /** The TypeInformation of the key. */
    private transient TypeInformation<K> keyTypeInfo;

    private transient ContextImpl context;

    private transient OnTimerContextImpl onTimerContext;

    private transient PythonTypeUtils.DataConverter<K, Object> keyConverter;

    private CepOperator<IN, K, OUT> internalOperator;

    private Pattern patternFromPython;

    ExecutionConfig getCepExecutionConfig() {
        return executionConfig;
    }

    private ExecutionConfig executionConfig;
    private TypeInformation<IN> inputTypeInfo;

    @Override
    protected <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        return super.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
    }

    public EmbeddedPythonCepOperator(
            Configuration config,
            ExecutionConfig executionConfig,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo,
            Pattern inputPattern) {
        super(config, pythonFunctionInfo, inputTypeInfo, outputTypeInfo);
        this.executionConfig = executionConfig;
        this.inputTypeInfo = inputTypeInfo;

        final boolean isProcessingTime = true;

        final boolean timeoutHandling = false;
        patternFromPython = inputPattern;
        Pattern<IN, ?> pattern = parsePattern();
        final NFACompiler.NFAFactory<IN> nfaFactory =
                NFACompiler.compileFactory(pattern, timeoutHandling);

        PatternProcessFunction<IN, OUT> processFunction =
                new PatternProcessFunction<IN, OUT>() {
                    @Override
                    public void processMatch(
                            Map<String, List<IN>> match, Context ctx, Collector<OUT> out)
                            throws Exception {
                        StringBuilder sb = new StringBuilder();
                        for (String k : match.keySet()) {
                            for (IN i : match.get(k)) {
                                sb.append(k)
                                        .append(": ")
                                        .append((String) ((Row) i).getField(0))
                                        .append(",")
                                        .append( ((Row) i).getField(1));
                            }
                        }
                        out.collect((OUT) Row.of(sb.toString(), 1));
                    }
                };
        internalOperator =
                new CepOperator<IN, K, OUT>(
                        inputTypeInfo.createSerializer(executionConfig),
                        isProcessingTime,
                        nfaFactory,
                        null,
                        AfterMatchSkipStrategy.noSkip(),
                        processFunction,
                        null);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        internalOperator.setup(containingTask, config, output);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        internalOperator.initializeState(context);
    }

    @Override
    public void open() throws Exception {

        keyTypeInfo = ((RowTypeInfo) this.getInputTypeInfo()).getTypeAt(0);
        //        throw new RuntimeException(keyTypeInfo.toString());
        keyConverter = PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(keyTypeInfo);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl(timerService);

        onTimerContext = new OnTimerContextImpl(timerService);

        super.open();
        final boolean timeoutHandling = false;
        Pattern<IN, ?> pattern = parsePattern();

        final NFACompiler.NFAFactory<IN> nfaFactory =
                NFACompiler.compileFactory(pattern, timeoutHandling);

        internalOperator.setTimerService(internalTimerService);
        internalOperator.setNfaFactory(nfaFactory);
        internalOperator.open();
        internalOperator.setProcessingTimeService(this.getProcessingTimeService());
    }

    @Override
    public List<FlinkFnApi.UserDefinedDataStreamFunction> createUserDefinedFunctionsProto() {
        return ProtoUtils.createUserDefinedDataStreamStatefulFunctionProtos(
                getPythonFunctionInfo(),
                getRuntimeContext(),
                getJobParameters(),
                keyTypeInfo,
                inBatchExecutionMode(getKeyedStateBackend()),
                config.get(PYTHON_METRIC_ENABLED),
                config.get(PYTHON_PROFILE_ENABLED),
                hasSideOutput,
                config.get(STATE_CACHE_SIZE),
                config.get(MAP_STATE_READ_CACHE_SIZE),
                config.get(MAP_STATE_WRITE_CACHE_SIZE));
    }

    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        invokeUserFunction(TimeDomain.EVENT_TIME, timer);
        internalOperator.onEventTime(timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
        internalOperator.onProcessingTime(timer);
    }

    @Override
    public Object getFunctionContext() {
        return context;
    }

    @Override
    public Object getTimerContext() {
        return onTimerContext;
    }

    @Override
    public <T> AbstractEmbeddedDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new EmbeddedPythonCepOperator<>(
                config,
                getCepExecutionConfig(),
                pythonFunctionInfo,
                getInputTypeInfo(),
                outputTypeInfo,
                patternFromPython);
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<K, VoidNamespace> timer)
            throws Exception {
        onTimerContext.timeDomain = timeDomain;
        onTimerContext.timer = timer;
        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod("operation", "on_timer", timer.getTimestamp());

        while (results.hasNext()) {
            OUT result = outputDataConverter.toInternal(results.next());
            collector.collect(result);
        }
        results.close();

        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        timestamp = element.getTimestamp();
        LOG.info("???");
        LOG.info(String.valueOf(element.getValue()));
        internalOperator.processElement(element);
    }

    private class ContextImpl {

        private final TimerService timerService;

        ContextImpl(TimerService timerService) {
            this.timerService = timerService;
        }

        public long timestamp() {
            return timestamp;
        }

        public TimerService timerService() {
            return timerService;
        }

        @SuppressWarnings("unchecked")
        public Object getCurrentKey() {
            return keyConverter.toExternal(
                    (K) ((Row) EmbeddedPythonCepOperator.this.getCurrentKey()).getField(0));
        }
    }

    private class OnTimerContextImpl {

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<K, VoidNamespace> timer;

        OnTimerContextImpl(TimerService timerService) {
            this.timerService = timerService;
        }

        public long timestamp() {
            return timer.getTimestamp();
        }

        public TimerService timerService() {
            return timerService;
        }

        public int timeDomain() {
            return timeDomain.ordinal();
        }

        @SuppressWarnings("unchecked")
        public Object getCurrentKey() {
            return keyConverter.toExternal((K) ((Row) timer.getKey()).getField(0));
        }
    }

    private Pattern parsePattern() {
        Pattern<IN, ?> pattern = null;
        List<Pattern> patterns = new ArrayList<>();

        Pattern curPattern = patternFromPython;
        while (curPattern != null) {
            patterns.add(curPattern);
            curPattern = curPattern.getPrevious();
        }
        for (int i = patterns.size() - 1; i >= 0; i--) {
            if (i == patterns.size() - 1) {
                pattern =
                        Pattern.<IN>begin(patterns.get(i).getName())
                                .where(
                                        new SimplePythonCondition<>(
                                                patterns.get(i).getName(),
                                                this.interpreter,
                                                PythonTypeUtils.TypeInfoToDataConverter
                                                        .typeInfoDataConverter(inputTypeInfo),
                                                outputDataConverter))
                                .times(patterns.get(i).getTimes().getFrom());
            } else {
                assert pattern != null;
                assert patterns.get(i) != null;
                assert patterns.get(i).getTimes() != null;
                pattern =
                        pattern.followedBy(patterns.get(i).getName())
                                .where(
                                        new SimplePythonCondition<>(
                                                patterns.get(i).getName(),
                                                this.interpreter,
                                                PythonTypeUtils.TypeInfoToDataConverter
                                                        .typeInfoDataConverter(inputTypeInfo),
                                                outputDataConverter))
                                .times(patterns.get(i).getTimes().getFrom());
            }
        }
        //        throw new RuntimeException(String.valueOf(pattern));
        //

        return pattern;
    }
}
