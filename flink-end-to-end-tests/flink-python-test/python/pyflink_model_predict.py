from apache_beam.ml.inference.huggingface_inference import PipelineTask
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf
from transformers import pipeline
# # 从 ModelScope 下载模型
# model_dir = snapshot_download("google/flan-t5-small")
# print(model_dir)

import logging

class MyPythonMLUDTF(TableFunction):
    def __init__(self):
        self.model = None

    def open(self, runtime_context):
        logging.info(f"Runtime context: {runtime_context}")
        print(f"Runtime context: {runtime_context.__dict__} ")
        self.model = runtime_context.get_job_parameter("model", "empty")

    def eval(self, content: str):
        """
        核心逻辑：输入一个字符串，使用 yield 返回多行结果。
        """
        if content:
            # 假设以逗号分隔
            # str.upper(content)
            # for s in content.split(","):
            # yield 直接返回数据，Flink 会自动将其封装成 Row
            if self.model:
                yield content + " is nothing but a joke " + self.model, 1.0
            else:
                yield content + " is nothing but a joke", 1.0
MyPythonMLUDTF_func = udtf(MyPythonMLUDTF(), result_types=[DataTypes.STRING(), DataTypes.DOUBLE()])
#
# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#
#     # 构造一些文本数据
#     texts = [
#         "translate English to Spanish: I love Flink and PyFlink, it is amazing!",
#         "translate English to Spanish: This movie was terrible and boring.",
#         "translate English to Spanish: The food was delicious, I will come back again."
#     ]
#
#     ds = env.from_collection(texts, type_info=Types.STRING())
#
#     # 调用 Hugging Face 模型
#     result_ds = ds.map(HuggingFaceMap("/Users/kenken/.cache/modelscope/hub/models/google/flan-t5-small"), output_type=Types.STRING())
#
#     # 输出结果
#     result_ds.print()
#
#     env.execute("PyFlink HuggingFace Example")
#
# if __name__ == '__main__':
#     main()
