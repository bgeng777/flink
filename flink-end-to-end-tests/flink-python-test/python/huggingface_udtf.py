import logging

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

    def eval(self, content: str, comment: str):
        if content:
            if self.model:
                output = self.pipeline(content)[0]["generated_text"]
                yield output, len(output)
            else:
                yield "no model specified", 0


HuggingFaceModelUDTF = udtf(HuggingFaceFunc())
