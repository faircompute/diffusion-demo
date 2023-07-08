import torch
from diffusers import DiffusionPipeline

DiffusionPipeline.download("runwayml/stable-diffusion-v1-5", torch_dtype=torch.float16, revision="fp16")