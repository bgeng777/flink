import logging
from pyflink.table import DataTypes
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
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=True)

        self.model = AutoModelForCausalLM.from_pretrained(
            model_dir,
            device_map=device
        )
        self.pipeline = pipeline("text-generation", model=self.model, tokenizer=self.tokenizer)

    def eval(self, content: str, comment: str):
        if content:
            if self.model:
                output = self.pipeline(content)[0]["generated_text"]
                yield output, len(output)
            else:
                yield "no model specified", 0


HuggingFaceMLUDTF = udtf(HuggingFaceFunc(), result_types=[DataTypes.STRING(), DataTypes.INT()])

#
# # 1. 指定本地模型目录（改成你的实际路径）
# model_dir = os.path.expanduser("~/.cache/modelscope/hub/models/Qwen/Qwen3-0.6B")
#
# # 2. 加载 tokenizer 和模型
# tokenizer = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=True)
# model = AutoModelForCausalLM.from_pretrained(
#     model_dir,
#     torch_dtype=torch.float16,  # 如果没有 GPU 或显存小可以改为 torch.float32
#     device_map="auto"           # 自动把模型放到 GPU/CPU
# )
#
# # 3. 创建 pipeline
# text_gen = pipeline(
#     "text-generation",
#     model=model,
#     tokenizer=tokenizer
# )
#
# # 4. 对给定输入做推理
# prompt = "请直接回答以下问题，不要展示你的思考过程，不要使用 <|thinking|> 标签。 问题：你好，请用一句话介绍一下你自己。"
# outputs = text_gen(prompt)
#
# print(outputs[0]["generated_text"])
