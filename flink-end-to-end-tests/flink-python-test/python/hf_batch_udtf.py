import logging

import pandas as pd

from pandas import Series
from pyflink.table.udf import TableFunction, udtf
from pyflink.table.ml import BatchPredictFunction
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline


class HFBatchPredict(BatchPredictFunction):
    def __init__(self):
        self.model = None
        self.tokenizer = None
        self.pipeline = None

    def open(self, runtime_context):
        logging.info(f"Runtime context: {runtime_context.__dict__}")
        model_dir = runtime_context.get_job_parameter("model", None)
        device = runtime_context.get_job_parameter("device_map", "auto")

        if model_dir is None:
            raise RuntimeError(f"No model specified")
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_dir, trust_remote_code=True
        )

        self.model = AutoModelForCausalLM.from_pretrained(model_dir, device_map=device)
        self.pipeline = pipeline(
            "text-generation", model=self.model, tokenizer=self.tokenizer
        )
        assert self.model is not None


    def predict(self, data: pd.DataFrame):
        print(f"debug data: {data} ")
        content =  data['col0']
        comment = data['col1']
        print(f"debug eval: {content} {comment}")

        outputs = self.pipeline(content.tolist())
        results = [
            item[0]["generated_text"]
            for item in outputs
        ]
        # results = content.tolist()
        # print(results)
        return  pd.DataFrame({ 'c0': results, 'c1': comment})
