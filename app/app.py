import logging
import re
import os
import sys
import time
import warnings

import torch
from diffusers import DiffusionPipeline


def set_global_logging_level(level=logging.ERROR, prefixes=None):
    """
    Override logging levels of different modules based on their name as a prefix.
    It needs to be invoked after the modules have been loaded so that their loggers have been initialized.

    Args:
        - level: desired level. e.g. logging.INFO. Optional. Default is logging.ERROR
        - prefixes: list of one or more str prefices to match (e.g. ["transformers", "torch"]). Optional.
                    Default is None to match all active loggers.
                    The match is a case-sensitive `module_name.startswith(prefix)`
    """
    prefix_re = re.compile(fr'^(?:{ "|".join(prefixes if prefixes else []) })')
    for name in logging.root.manager.loggerDict:
        if re.match(prefix_re, name):
            logging.getLogger(name).setLevel(level)


if os.environ.get("QUIET", False):
    set_global_logging_level()
    warnings.filterwarnings("ignore")

start_time = time.time()
print("Loading model...")
pipe = DiffusionPipeline.from_pretrained("runwayml/stable-diffusion-v1-5", torch_dtype=torch.float16, revision="fp16")
if not os.environ.get("WARMUP", False):
    pipe = pipe.to('cuda')
    print(f'Model loaded in {(time.time() - start_time) * 1000:.2f}ms')
    prompt = sys.stdin.readline()
    print("Prompt:", prompt)
    print(f"Running on '{torch.cuda.get_device_name()}'")
    start_time = time.time()
    image = pipe(prompt, num_inference_steps=30).images[0]
    print(f"Inference done in {(time.time() - start_time) * 1000:.2f}ms")
    image.save("result.png")