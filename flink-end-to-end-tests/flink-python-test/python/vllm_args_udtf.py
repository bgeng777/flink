import dataclasses
import logging
import shlex
from typing import List

import msgspec
import pandas as pd
from pyflink.common import Row
from pyflink.table.ml import PredictFunction, BatchPredictFunction
from pyflink.table.udf import TableFunction, udtf
from vllm import LLM, SamplingParams
from vllm.engine.arg_utils import EngineArgs
from vllm.utils.argparse_utils import FlexibleArgumentParser


# 暴露UDTF
class VLLMFunc(TableFunction):
    def __init__(self):
        self.model = None
        self.sampling_params = None

    def open(self, runtime_context):
        logging.info(f"Runtime context: {runtime_context}")
        model_dir = runtime_context.get_job_parameter("model", "empty")
        if model_dir is None:
            raise RuntimeError(f"No model directory specified")

        cli_args_string = runtime_context.get_job_parameter(
            "vllm.engine_args", None
        )
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
        params_str = runtime_context.get_job_parameter(
            "vllm.sampling_params", None
        )

        if params_str is None:
            self.sampling_params = SamplingParams()
        else:
            decoder = msgspec.json.Decoder(SamplingParams)
            self.sampling_params = decoder.decode(params_str)

    def eval(self, prompt: str, comment: str):
        logging.info(f"eval promt: {prompt}")
        print(f"eval promt: {prompt}")
        if prompt:
            if self.model:
                outputs = self.model.generate(prompt, self.sampling_params)
                generated_text = outputs[0].outputs[0].text
                print(f"Generated text: {generated_text}")
                return [generated_text, len(generated_text)]
            else:
                return ["no model specified", 0]


VLLMMLUDTF = udtf(VLLMFunc())

# 不暴露UDTF，引入新的PredictFunction
class VLLMFunc(PredictFunction):
    def __init__(self):
        self.model = None
        self.sampling_params = None

    def open(self, runtime_context):
        logging.info(f"Runtime context: {runtime_context}")
        model_dir = runtime_context.get_job_parameter("model", "empty")
        if model_dir is None:
            raise RuntimeError(f"No model directory specified")

        cli_args_string = runtime_context.get_job_parameter(
            "vllm.engine_args", None
        )
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
        params_str = runtime_context.get_job_parameter(
            "vllm.sampling_params", None
        )

        if params_str is None:
            self.sampling_params = SamplingParams()
        else:
            decoder = msgspec.json.Decoder(SamplingParams)
            self.sampling_params = decoder.decode(params_str)

    # def eval(self, prompt: str, comment: str):
    #     logging.info(f"eval promt: {prompt}")
    #     print(f"eval promt: {prompt}")
    #     if prompt:
    #         if self.model:
    #             outputs = self.model.generate(prompt, self.sampling_params)
    #             generated_text = outputs[0].outputs[0].text
    #             print(f"Generated text: {generated_text}")
    #             return [generated_text, len(generated_text)]
    #         else:
    #             return ["no model specified", 0]
    def predict(self, data: Row) -> List[Row]:
        prompt = data[0]
        logging.info(f"eval promt: {prompt}")
        print(f"eval promt: {prompt}")
        if prompt:
            if self.model:
                outputs = self.model.generate(prompt, self.sampling_params)
                generated_text = outputs[0].outputs[0].text
                print(f"Generated text: {generated_text}")
                return [(generated_text, len(generated_text))]
            else:
                return [ ("no model specified", 0) ]



class VLLMBatchFunc(BatchPredictFunction):
    def __init__(self):
        self.model = None
        self.sampling_params = None

    def open(self, runtime_context):
        logging.info(f"Runtime context: {runtime_context}")
        model_dir = runtime_context.get_job_parameter("model", "empty")
        if model_dir is None:
            raise RuntimeError(f"No model directory specified")

        cli_args_string = runtime_context.get_job_parameter(
            "vllm.engine_args", None
        )
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
        params_str = runtime_context.get_job_parameter(
            "vllm.sampling_params", None
        )

        if params_str is None:
            self.sampling_params = SamplingParams()
        else:
            decoder = msgspec.json.Decoder(SamplingParams)
            self.sampling_params = decoder.decode(params_str)


    def predict(self, data: pd.DataFrame):
        content =  data['col0']
        comment = data['col1']
        outputs = self.model.generate(content.tolist(), self.sampling_params)
        results = [
            output.outputs[0].text
            for output in outputs
        ]
        return  pd.DataFrame({ 'c0': results, 'c1': 10 * len(results)})
