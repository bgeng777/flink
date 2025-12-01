import contextlib
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
import dataclasses
import argparse
import shlex
from typing import Optional, List

# 从 vLLM 导入关键组件
from vllm import LLM, SamplingParams
from vllm.engine.arg_utils import EngineArgs
from vllm.utils.argparse_utils import FlexibleArgumentParser # 推荐使用 vLLM 的解析器


class VLLMFunc(TableFunction):
    def __init__(self):
        self.model : LLM = None
        self.sampling_params = None


    def open(self, runtime_context):
        output_filename = "/Users/kenken/opensource/flink/flink-end-to-end-tests/flink-python-test/python/redirected_output.log"
        with open(output_filename, 'w', encoding='utf-8') as f:
            # 使用 contextlib.redirect_stdout 重定向 sys.stdout 到文件对象 f
            with contextlib.redirect_stdout(f):
                import os
                print(os.getcwd())

                print(f"2. 开始重定向：所有 print() 调用都写入文件 {output_filename}")
                logging.info(f"Runtime context: {runtime_context}")
                print(f"Runtime context: {runtime_context.__dict__} ")
                model_dir = runtime_context.get_job_parameter("model", "empty")
                if model_dir is None:
                    raise RuntimeError(f"No model directory specified")

                cli_args_string = runtime_context.get_job_parameter("vllm.args", None)
                if cli_args_string is None:
                    self.model = LLM(model=model_dir)
                    return
                args_list = shlex.split(cli_args_string)
                args_list.append("--model")
                args_list.append(model_dir)
                parser = FlexibleArgumentParser(
                    description="parsing vLLM EngineArgs in pyflink"
                )
                parser = EngineArgs.add_cli_args(parser)
                ns, args = parser.parse_known_args(args_list)
                engine_args = EngineArgs.from_cli_args(ns)

                self.model = LLM(**dataclasses.asdict(engine_args))
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
                outputs = self.model.generate( prompt, self.sampling_params)
                generated_text = outputs[0].outputs[0].text
                print(f"Generated text: {generated_text}")
                yield generated_text, len(generated_text)
            else:
                yield "no model specified", 0

    def close(self):
        self.model.close()
VLLMMLUDTF = udtf(VLLMFunc())
