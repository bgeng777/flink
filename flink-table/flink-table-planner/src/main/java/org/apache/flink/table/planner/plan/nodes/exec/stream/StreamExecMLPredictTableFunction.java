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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.functions.PythonPredictFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionInfoWithConfig;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.ml.PythonPredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.MLPredictCodeGenerator;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MLPredictSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.functions.ml.ModelPredictRuntimeProviderContext;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.ml.AsyncMLPredictRunner;
import org.apache.flink.table.runtime.operators.ml.MLPredictRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonCorrelate.PYTHON_TABLE_FUNCTION_OPERATOR_NAME;

/** Stream {@link ExecNode} for {@code ML_PREDICT}. */
@ExecNodeMetadata(
        name = "stream-exec-ml-predict-table-function",
        version = 1,
        consumedOptions = {
            "table.exec.async-ml-predict.max-concurrent-operations",
            "table.exec.async-ml-predict.timeout",
            "table.exec.async-ml-predict.output-mode"
        },
        producedTransformations = StreamExecMLPredictTableFunction.ML_PREDICT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_1,
        minStateVersion = FlinkVersion.v2_1)
public class StreamExecMLPredictTableFunction extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData>, StreamExecNode<RowData> {

    public static final String ML_PREDICT_TRANSFORMATION = "ml-predict-table-function";

    public static final String FIELD_NAME_ML_PREDICT_SPEC = "mlPredictSpec";
    public static final String FIELD_NAME_MODEL_SPEC = "modelSpec";
    public static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    @JsonProperty(FIELD_NAME_ML_PREDICT_SPEC)
    private final MLPredictSpec mlPredictSpec;

    @JsonProperty(FIELD_NAME_MODEL_SPEC)
    private final ModelSpec modelSpec;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    private final @Nullable FunctionCallUtil.AsyncOptions asyncOptions;

    public StreamExecMLPredictTableFunction(
            ReadableConfig persistedConfig,
            MLPredictSpec mlPredictSpec,
            ModelSpec modelSpec,
            @Nullable FunctionCallUtil.AsyncOptions asyncOptions,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMLPredictTableFunction.class),
                persistedConfig,
                mlPredictSpec,
                modelSpec,
                asyncOptions,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecMLPredictTableFunction(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_ML_PREDICT_SPEC) MLPredictSpec mlPredictSpec,
            @JsonProperty(FIELD_NAME_MODEL_SPEC) ModelSpec modelSpec,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable
                    FunctionCallUtil.AsyncOptions asyncOptions,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.mlPredictSpec = mlPredictSpec;
        this.modelSpec = modelSpec;
        this.asyncOptions = asyncOptions;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);

        ModelProvider provider = modelSpec.getModelProvider(planner.getFlinkContext());
        boolean async = asyncOptions != null;
        boolean isPython = provider instanceof PythonPredictRuntimeProvider;
        UserDefinedFunction predictFunction;
        if (isPython) {
            predictFunction = findPythonModelFunction(provider, async);
        } else {
            predictFunction = findModelFunction(provider, async);
        }
        FlinkContext context = planner.getFlinkContext();
        DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();

        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
        RowType modelOutputType =
                (RowType)
                        modelSpec
                                .getContextResolvedModel()
                                .getResolvedModel()
                                .getResolvedOutputSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();

        if (isPython) {
            final Configuration pythonConfig =
                    CommonPythonUtil.extractPythonConfiguration(
                            planner.getTableConfig(), planner.getFlinkContext().getClassLoader());
            return createPythonModelPredict(
                    inputTransformation,
                    config,
                    planner.getFlinkContext().getClassLoader(),
                    pythonConfig,
                    inputType,
                    modelOutputType,
                    (PythonPredictFunction) predictFunction);
        }

        return async
                ? createAsyncModelPredict(
                        inputTransformation,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        dataTypeFactory,
                        inputType,
                        modelOutputType,
                        (RowType) getOutputType(),
                        (AsyncPredictFunction) predictFunction)
                : createModelPredict(
                        inputTransformation,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        dataTypeFactory,
                        inputType,
                        modelOutputType,
                        (RowType) getOutputType(),
                        (PredictFunction) predictFunction);
    }

    private Transformation<RowData> createModelPredict(
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config,
            ClassLoader classLoader,
            DataTypeFactory dataTypeFactory,
            RowType inputRowType,
            RowType modelOutputType,
            RowType resultRowType,
            PredictFunction predictFunction) {
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                MLPredictCodeGenerator.generateSyncPredictFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        modelOutputType,
                        resultRowType,
                        mlPredictSpec.getFeatures(),
                        predictFunction,
                        modelSpec.getContextResolvedModel().getIdentifier().asSummaryString(),
                        config.get(PipelineOptions.OBJECT_REUSE));
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                MLPredictCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, classLoader),
                        inputRowType,
                        modelOutputType,
                        (RowType) getOutputType());
        MLPredictRunner mlPredictRunner = new MLPredictRunner(generatedFetcher, generatedCollector);
        SimpleOperatorFactory<RowData> operatorFactory =
                SimpleOperatorFactory.of(new ProcessOperator<>(mlPredictRunner));
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism(),
                false);
    }

    @SuppressWarnings("unchecked")
    private Transformation<RowData> createAsyncModelPredict(
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config,
            ClassLoader classLoader,
            DataTypeFactory dataTypeFactory,
            RowType inputRowType,
            RowType modelOutputType,
            RowType resultRowType,
            AsyncPredictFunction asyncPredictFunction) {
        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFuncWithType =
                        MLPredictCodeGenerator.generateAsyncPredictFunction(
                                config,
                                classLoader,
                                dataTypeFactory,
                                inputRowType,
                                modelOutputType,
                                resultRowType,
                                mlPredictSpec.getFeatures(),
                                asyncPredictFunction,
                                modelSpec
                                        .getContextResolvedModel()
                                        .getIdentifier()
                                        .asSummaryString());
        AsyncFunction<RowData, RowData> asyncFunc =
                new AsyncMLPredictRunner(
                        (GeneratedFunction) generatedFuncWithType.tableFunc(),
                        Preconditions.checkNotNull(asyncOptions).asyncBufferCapacity);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                new AsyncWaitOperatorFactory<>(
                        asyncFunc,
                        asyncOptions.asyncTimeout,
                        asyncOptions.asyncBufferCapacity,
                        asyncOptions.asyncOutputMode),
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism(),
                false);
    }

    private Transformation<RowData> createPythonModelPredict(
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonEnvConfig,
            RowType inputRowType,
            RowType modelOutputType,
            PythonPredictFunction pythonPredictFunction) {
        List<Integer> featureIndices = getFeatureIndices();
        Object[] inputs = featureIndices.toArray(new Integer[0]);
        PythonFunction pythonFunction = pythonPredictFunction.createPythonFunction(pythonEnvConfig);
        PythonFunctionInfo pythonFunctionInfo =
                new PythonFunctionInfoWithConfig(
                        pythonFunction, inputs, pythonPredictFunction.getModelConfig());

        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonTableFunctionOperator(
                        config,
                        classLoader,
                        pythonEnvConfig,
                        inputRowType,
                        modelOutputType,
                        pythonFunctionInfo,
                        featureIndices);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(ML_PREDICT_TRANSFORMATION, config),
                pythonOperator,
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism(),
                false);
    }

    private List<Integer> getFeatureIndices() {
        List<FunctionCallUtil.FunctionParam> features = mlPredictSpec.getFeatures();
        List<Integer> featureIndices = new ArrayList<>();
        for (FunctionCallUtil.FunctionParam param : features) {
            if (param instanceof FunctionCallUtil.FieldRef) {
                FunctionCallUtil.FieldRef fieldRef = (FunctionCallUtil.FieldRef) param;
                featureIndices.add(fieldRef.index);
            } else {
                throw new TableException(
                        String.format("Unknown operand for descriptor operator: %s.", param));
            }
        }
        return featureIndices;
    }

    private OneInputStreamOperator<RowData, RowData> getPythonTableFunctionOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig,
            RowType inputType,
            RowType modelOutputType,
            PythonFunctionInfo pythonFunctionInfo,
            List<Integer> udtfInputIndices) {
        int[] udtfInputIndicesArr = udtfInputIndices.stream().mapToInt(Integer::intValue).toArray();
        boolean isInProcessMode =
                CommonPythonUtil.isPythonWorkerInProcessMode(pythonConfig, classLoader);
        final RowType udfInputType =
                (RowType) Projection.of(udtfInputIndicesArr).project(inputType);
        // Note: CommonExecPythonCorrelate.getPythonTableFunctionOperator extracts udfOutputType via
        // projection on `outputType`.
        // Codes look like:
        //        final RowType udfOutputType  =
        //                (RowType)Projection.range(inputType.getFieldCount(),
        // outputType.getFieldCount())
        //                        .project(outputType);
        // It works as UDTF can be considered as joining 2 tables but ML_PREDICT provide
        // `modelOutputType` directly so here we use it as the source of truth.

        try {
            if (isInProcessMode) {
                Class<?> clazz =
                        CommonPythonUtil.loadClass(
                                PYTHON_TABLE_FUNCTION_OPERATOR_NAME, classLoader);
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                PythonFunctionInfo.class,
                                RowType.class,
                                RowType.class,
                                RowType.class,
                                FlinkJoinType.class,
                                GeneratedProjection.class);
                return (OneInputStreamOperator<RowData, RowData>)
                        ctor.newInstance(
                                pythonConfig,
                                pythonFunctionInfo,
                                inputType,
                                udfInputType,
                                modelOutputType,
                                FlinkJoinType.INNER,
                                ProjectionCodeGenerator.generateProjection(
                                        new CodeGeneratorContext(config, classLoader),
                                        "UdtfInputProjection",
                                        inputType,
                                        udfInputType,
                                        udtfInputIndicesArr));
            } else {
                throw new UnsupportedOperationException("Thread mode is supported.");
            }
        } catch (Exception e) {
            throw new TableException("Python Table Function Operator constructed failed.", e);
        }
    }

    private UserDefinedFunction findModelFunction(ModelProvider provider, boolean async) {
        ModelPredictRuntimeProviderContext context =
                new ModelPredictRuntimeProviderContext(
                        modelSpec.getContextResolvedModel().getResolvedModel(),
                        Configuration.fromMap(mlPredictSpec.getRuntimeConfig()));
        if (async) {
            if (provider instanceof AsyncPredictRuntimeProvider) {
                return ((AsyncPredictRuntimeProvider) provider).createAsyncPredictFunction(context);
            }
        } else {
            if (provider instanceof PredictRuntimeProvider) {
                return ((PredictRuntimeProvider) provider).createPredictFunction(context);
            }
        }

        throw new TableException(
                "Required "
                        + (async ? "async" : "sync")
                        + " model function by planner, but ModelProvider "
                        + "does not offer a valid model function.");
    }

    private UserDefinedFunction findPythonModelFunction(ModelProvider provider, boolean async) {
        ModelPredictRuntimeProviderContext context =
                new ModelPredictRuntimeProviderContext(
                        modelSpec.getContextResolvedModel().getResolvedModel(),
                        Configuration.fromMap(mlPredictSpec.getRuntimeConfig()));

        if (async) {
            throw new TableException("Async python model function is not supported yet.");
        } else {
            if (provider instanceof PythonPredictRuntimeProvider) {
                return ((PythonPredictRuntimeProvider) provider)
                        .createPythonPredictFunction(context);
            }
        }

        throw new TableException(
                "Required python model function by planner, but ModelProvider "
                        + "does not offer a valid model function.");
    }
}
