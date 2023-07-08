FROM pytorch/pytorch:1.13.1-cuda11.6-cudnn8-runtime

# Information on Stable Diffusion optimization
# https://huggingface.co/docs/diffusers/optimization/torch2.0
# https://github.com/huggingface/diffusers/pull/3313
RUN pip3 install --no-cache-dir accelerate==0.20.3 diffusers==0.17.1 huggingface==0.0.1 transformers==4.30.2

# Load weights, use separate file to avoid recreating layer every time app.py is changed
COPY app_download_weights.py /workspace/app_download_weights.py
RUN python3 /workspace/app_download_weights.py

# PyTorch with CUDA 11.8
# This will stable diffusion 2X faster on newer GPUs, but 2X slower on older GPUs
# RUN pip3 install --no-cache-dir --upgrade --force-reinstall torch==2.0.1 --index-url https://download.pytorch.org/whl/cu118

# Warm up the cache, compile model, etc.
COPY app.py /workspace/app.py
RUN WARMUP=1 python3 /workspace/app.py

ENV QUIET=1
ENTRYPOINT ["python3", "-u", "/workspace/app.py"]