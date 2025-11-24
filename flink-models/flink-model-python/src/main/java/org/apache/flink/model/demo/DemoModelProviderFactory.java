package org.apache.flink.model.demo;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DemoModelProviderFactory implements ModelProviderFactory {

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<String> MODEL =
            ConfigOptions.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder().text("Model path or model name").build());

    @Override
    public ModelProvider createModelProvider(Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();
        String model = helper.getOptions().get(MODEL);
        return new DemoModelProvider(new DemoModelPredictFunction(model));
    }

    @Override
    public String factoryIdentifier() {
        return "demo";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(MODEL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    public static class DemoModelProvider implements PredictRuntimeProvider {

        private final PredictFunction function;

        public DemoModelProvider(PredictFunction function) {
            this.function = function;
        }

        @Override
        public PredictFunction createPredictFunction(Context context) {
            return function;
        }

        @Override
        public ModelProvider copy() {
            return new DemoModelProvider(function);
        }
    }

    /** Values Predict function. */
    public static class DemoModelPredictFunction extends PredictFunction {

        private final String model;

        public DemoModelPredictFunction(String model) {

            this.model = model;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
        }

        @Override
        public Collection<RowData> predict(RowData features) {
            List<RowData> result = new ArrayList<>();
            GenericRowData rowData = new GenericRowData(1);
            rowData.setField(0, BinaryStringData.fromString(features.getString(0) + ": " + model));
            result.add(rowData);
            return Preconditions.checkNotNull(result);
        }
    }
}
