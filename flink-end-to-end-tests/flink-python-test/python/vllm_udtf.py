import logging
import threading
from collections import deque

from absl.logging import exception
from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch
import os
import os
from vllm import LLMEngine, SamplingParams, EngineArgs, AsyncEngineArgs
from vllm.utils import random_uuid
import asyncio
from vllm import AsyncLLMEngine, SamplingParams, EngineArgs
from vllm import LLM
from typing import List, Tuple
from vllm.utils import random_uuid


class VLLMFunc(TableFunction):
    def __init__(self):
        self.model : LLM = None
        self.sampling_params = None


    def open(self, runtime_context):

        logging.info(f"Runtime context: {runtime_context}")
        print(f"Runtime context: {runtime_context.__dict__} ")
        model_dir = runtime_context.get_job_parameter("model", "empty")
        self.model = LLM(model=model_dir,
                         max_model_len=256,
                         trust_remote_code=True,
                         dtype="half",
                         enforce_eager=True)
        self.sampling_params = SamplingParams(
            temperature=0.7,
            top_p=0.9,
            max_tokens=256
        )

    def eval(self, prompt: str, comment: str):
        logging.info(f"eval promt: {prompt}")
        print(f"eval promt: {prompt}")
        if prompt:
            if self.model:
                outputs = self.model.generate(prompt, self.sampling_params)
                generated_text = outputs[0].outputs[0].text
                print(f"Generated text: {generated_text}")
                yield generated_text, len(generated_text)
            else:
                yield "no model specified", 0
VLLMMLUDTF = udtf(VLLMFunc(), result_types=[DataTypes.STRING(), DataTypes.INT()])

# Codes from gpt:
import os
# from vllm import LLM, SamplingParams
#
# def initialize_llm_engine():
#     try:
#         # LLM 类是 vLLM 的同步入口，它封装了 LLMEngine
#         llm = LLM(model="/Users/kenken/.cache/modelscope/hub/models/Qwen/Qwen3-0.6B",
#                          max_model_len=256,
#                          trust_remote_code=True,
#                          dtype="half",
#                          enforce_eager=True)
#         return llm
#
#     except Exception as e:
#         print(f"❌ 引擎初始化失败，请检查模型路径和配置。错误: {e}")
#         # 如果遇到 Failed core proc(s) 错误，请检查前面的根原因日志。
#         return None
#
#
# # --- 3. 定义推理函数 ---
#
# def run_inference(llm: LLM):
#     """
#     使用 LLM 接口进行批量同步推理。
#     """
#     if llm is None:
#         return
#
#     # 定义采样参数
#     sampling_params = SamplingParams(
#         temperature=0.7,
#         top_p=0.9,
#         max_tokens=256,
#         # stop=["<|im_end|>"] # Qwen的停止词
#     )
#
#     # 定义批量输入的 Prompt 列表
#     prompts = [
#         "请用一句话介绍什么是量子计算。",
#         "写一首关于秋天的五言绝句。",
#         "Python中asyncio的作用是什么？",
#         "什么是LLM？",
#     ]
#
#     print(f"\n📨 正在提交 {len(prompts)} 个同步推理请求...")
#
#     # 核心调用：llm.generate() 接受一个 prompts 列表，并返回一个 RequestOutput 列表
#     # 这一调用会同步阻塞，直到所有请求都完成。
#     try:
#         outputs = llm.generate(
#             prompts,
#             sampling_params,
#             # lo_ra_request=LoRARequest("lora_name", 1) # 如果需要 LoRA
#         )
#
#         print("✅ 所有请求处理完毕。")
#
#         # 打印结果
#         print("\n--- 结果输出 ---")
#         for output in outputs:
#             prompt = output.prompt
#             # output.outputs 是一个列表，包含不同的生成结果（如果 num_beams > 1）
#             generated_text = output.outputs[0].text
#
#             print(f"--- Request ID: {output.request_id} ---")
#             print(f"**输入 (Prompt):** {prompt}")
#             print(f"**输出 (Generated):**\n{generated_text.strip()}\n")
#
#     except Exception as e:
#         print(f"❌ 推理过程中发生错误: {e}")
#
#
# # --- 4. 运行主程序 ---
#
# if __name__ == "__main__":
#     llm_instance = initialize_llm_engine()
#     if llm_instance:
#         run_inference(llm_instance)
