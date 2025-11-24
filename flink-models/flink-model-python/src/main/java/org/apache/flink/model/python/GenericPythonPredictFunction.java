package org.apache.flink.model.python;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.PythonPredictFunction;

import java.util.Map;

public class GenericPythonPredictFunction extends PythonPredictFunction {
    private static final long serialVersionUID = 1L;
    private final String pythonClass;
    private final Configuration modelConfig;

    public GenericPythonPredictFunction(
            String model, String pythonClass, Map<String, String> modelConfig) {
        this.pythonClass = pythonClass;
        this.modelConfig = Configuration.fromMap(modelConfig);
        this.modelConfig.set(GenericPythonOptions.MODEL, model);
    }

    @Override
    public String getPythonClass() {
        return pythonClass;
    }

    @Override
    public Configuration getModelConfig() {
        return modelConfig;
    }
}
