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

package org.apache.flink.table.runtime.operators.python.table.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionInfoWithConfig;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.python.PythonOptions.MAX_ARROW_BATCH_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createArrowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;

/** The Python {@link TableFunction} operator. */
@Internal
public class ArrowPythonPredictFunctionOperator
        extends AbstractStatelessFunctionOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final String TABLE_FUNCTION_URN = "flink:transform:table_function:v1";

    /** The Python {@link TableFunction} to be executed. */
    private final PythonFunctionInfo tableFunction;

    private final GeneratedProjection udtfInputGeneratedProjection;

    /** The collector used to collect records. */
    private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** The JoinedRowData reused holding the execution result. */
    private transient JoinedRowData reuseJoinedRow;

    /** The Projection which projects the udtf input fields from the input row. */
    private transient Projection<RowData, BinaryRowData> udtfInputProjection;

    /** The type serializer for the forwarded fields. */
    private transient RowDataSerializer forwardedInputSerializer;

    /** The current number of elements to be included in an arrow batch. */
    private transient int currentBatchCount;

    /** Max number of elements to include in an arrow batch. */
    private transient int maxArrowBatchSize;

    private transient ArrowSerializer arrowSerializer;
    protected final RowType wrapped;

    public ArrowPythonPredictFunctionOperator(
            Configuration config,
            PythonFunctionInfo tableFunction,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            FlinkJoinType joinType,
            GeneratedProjection udtfInputGeneratedProjection) {
        super(config, inputType, udfInputType, udfOutputType);
        this.tableFunction = Preconditions.checkNotNull(tableFunction);
        this.udtfInputGeneratedProjection =
                Preconditions.checkNotNull(udtfInputGeneratedProjection);
        this.wrapped = new RowType(
                java.util.Collections.singletonList(
                        new RowType.RowField("wrapped", udfOutputType)  // 一列，类型就是原来的 RowType
                )
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseJoinedRow = new JoinedRowData();

        udtfInputProjection =
                udtfInputGeneratedProjection.newInstance(
                        Thread.currentThread().getContextClassLoader());
        forwardedInputSerializer = new RowDataSerializer(inputType);

        maxArrowBatchSize = Math.min(config.get(MAX_ARROW_BATCH_SIZE), maxBundleSize);
        arrowSerializer = new ArrowSerializer(udfInputType, wrapped);
        arrowSerializer.open(bais, baos);
        currentBatchCount = 0;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return tableFunction.getPythonFunction().getPythonEnv();
    }

    @Override
    public String getFunctionUrn() {
        return TABLE_FUNCTION_URN;
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(RowType runnerInputType) {
        if (tableFunction.getPythonFunction().takesRowAsInput()) {
            throw new UnsupportedOperationException(
                    "The Python TableFunction Operator does not support row-based operations.");
        } else {
            return createArrowTypeCoderInfoDescriptorProto(
                    runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
        }
//        if (tableFunction.getPythonFunction().takesRowAsInput()) {
//            // need the field names in case of row-based operations
//            return createRowTypeCoderInfoDescriptorProto(
//                    runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
//        } else {
//            return createFlattenRowTypeCoderInfoDescriptorProto(
//                    runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
//        }
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(RowType runnerOutType) {
        RowType wrapped =  new RowType(
                java.util.Collections.singletonList(
                        new RowType.RowField("wrapped", runnerOutType)  // 一列，类型就是原来的 RowType
                )
        );
        return createArrowTypeCoderInfoDescriptorProto(
                wrapped, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        if (tableFunction instanceof PythonFunctionInfoWithConfig) {
            PythonFunctionInfoWithConfig pythonFunctionInfoWithConfig =
                    (PythonFunctionInfoWithConfig) tableFunction;
            Configuration udfConfig = pythonFunctionInfoWithConfig.getConfig();
            return ProtoUtils.createUserDefinedFunctionsProtoWithConfiguration(
                    getRuntimeContext(),
                    new PythonFunctionInfo[] {tableFunction},
                    config.get(PYTHON_METRIC_ENABLED),
                    config.get(PYTHON_PROFILE_ENABLED),
                    udfConfig);
        } else {
            return ProtoUtils.createUserDefinedFunctionsProto(
                    getRuntimeContext(),
                    new PythonFunctionInfo[] {tableFunction},
                    config.get(PYTHON_METRIC_ENABLED),
                    config.get(PYTHON_PROFILE_ENABLED));
        }
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        invokeCurrentBatch();
        super.invokeFinishBundle();
    }

    @Override
    public void endInput() throws Exception {
        invokeCurrentBatch();
        super.endInput();
    }

    @Override
    public void finish() throws Exception {
        invokeCurrentBatch();
        super.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (arrowSerializer != null) {
            arrowSerializer.close();
            arrowSerializer = null;
        }
    }

    private void invokeCurrentBatch() throws Exception {
        if (currentBatchCount > 0) {
            arrowSerializer.finishCurrentBatch();
            currentBatchCount = 0;
            pythonFunctionRunner.process(baos.toByteArray());
            checkInvokeFinishBundleByCount();
            baos.reset();
            arrowSerializer.resetWriter();
        }
    }

    @Override
    public void bufferInput(RowData input) {
        // always copy the input RowData
        RowData forwardedFields = forwardedInputSerializer.copy(input);
        forwardedFields.setRowKind(input.getRowKind());
        forwardedInputQueue.add(forwardedFields);
    }

    @Override
    public RowData getFunctionInput(RowData element) {
        return udtfInputProjection.apply(element);
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        arrowSerializer.write(getFunctionInput(value));
        currentBatchCount++;
        if (currentBatchCount >= maxArrowBatchSize) {
            invokeCurrentBatch();
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception {
        byte[] udfResult = resultTuple.f1;
        int length = resultTuple.f2;
        bais.setBuffer(udfResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            RowData input = forwardedInputQueue.poll();
            reuseJoinedRow.setRowKind(input.getRowKind());
            RowData wrappedData = arrowSerializer.read(i);
            RowData originalRow = wrappedData.getRow(0, udfOutputType.getFieldCount());
            rowDataWrapper.collect(reuseJoinedRow.replace(input, originalRow));
        }
        arrowSerializer.resetReader();
    }
}
