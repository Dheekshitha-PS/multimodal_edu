# Use a stable Python base
FROM python:3.10-slim

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies safely
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ffmpeg \
        git \
        wget \
        curl \
        unzip \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 && \
    rm -rf /var/lib/apt/lists/*

# Set working dir
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY app/ /app/

# Default command
CMD ["python", "download_and_upload.py"]
