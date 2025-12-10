import logging

import pandas as pd

from pandas import Series
from pyflink.table.udf import TableFunction, udtf
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline


class HuggingFaceFunc(TableFunction):
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

    # def eval(self, content: str, comment: str):
    #     print(f"debug eval: {content} {comment}")
    #
    #     output = self.pipeline(content)[0]["generated_text"]
    #     return [(output, len(output))]
    def eval(self, content: Series, comment: Series):
        print(f"debug eval: {content} {comment}")

        outputs = self.pipeline(content.tolist())
        results = [
            item[0]["generated_text"]
            for item in outputs
        ]
        print(results)
        return  pd.Series(results)
        # return List[str]
        # return [Series(Series()),  Series(Series())]
        # return pd.DataFrame(pd.Series(results), pd.Series(results))
        # [ DataFrame ], each dataframe is the result of an input row. each dataframe is m * n, m is the number of results for the given input row, n is the number of columns


        # output = self.pipeline(content)[0]["generated_text"]
        # return [(output, len(output))]



HuggingFaceModelUDTF = udtf(HuggingFaceFunc())
