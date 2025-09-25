import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

from source_urls import pdf_urls, youtube_urls,slides_urls

# Load environment variables
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

# Configure S3 client with extended timeouts and tuned transfers
S3_CONFIG = Config(
    read_timeout=300,
    connect_timeout=60,
    retries={"max_attempts": 5, "mode": "standard"},
)

TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=16 * 1024 * 1024,
    max_concurrency=2,
)

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
    config=S3_CONFIG,
)

print("Using bucket:", S3_BUCKET)
print("AWS Access Key exists:", bool(os.getenv("AWS_ACCESS_KEY_ID")))

def upload_to_s3(local_path, s3_key):
    print(f"Uploading {local_path} -> s3://{S3_BUCKET}/{s3_key}", flush=True)
    s3.upload_file(local_path, S3_BUCKET, s3_key, Config=TRANSFER_CONFIG)
    print(f"Uploaded {local_path} -> s3://{S3_BUCKET}/{s3_key}", flush=True)

def download_file(url, save_dir="downloads"):
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    local_filename = os.path.join(save_dir, os.path.basename(urlparse(url).path))
    print(f"Downloading {url}", flush=True)
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filename, "wb") as download:
            for chunk in response.iter_content(chunk_size=8192):
                download.write(chunk)
    print(f"Downloaded {local_filename}", flush=True)
    return local_filename

def download_youtube_video(url, save_dir="downloads/videos"):
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    print(f"Downloading YouTube video: {url}", flush=True)

    output_template = os.path.join(save_dir, "%(title)s.%(ext)s")

    try:
        subprocess.run(
            ["yt-dlp", "-f", "best[ext=mp4]", "-o", output_template, url],
            check=True,
        )
        print(f"Downloaded YouTube video to {save_dir}", flush=True)

        downloaded_files = sorted(Path(save_dir).glob("*.mp4"), key=os.path.getmtime)
        return str(downloaded_files[-1]) if downloaded_files else None

    except Exception as error:
        print(f"Failed to download {url}: {error}", flush=True)
        return None

if __name__ == "__main__":
    for url in pdf_urls:
        local_file = download_file(url)
        upload_to_s3(local_file, f"pdfs/{os.path.basename(local_file)}")

    for url in youtube_urls:
        local_video = download_youtube_video(url)
        if not local_video:
            continue
        upload_to_s3(local_video, f"videos/{os.path.basename(local_video)}")
    for url in slides_urls:
        local_file = download_file(url)
        if not local_file:
            continue
        upload_to_s3(local_file, f"slides/{os.path.basename(local_file)}")