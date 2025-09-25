import os
import json
import logging
import subprocess
import yt_dlp
import boto3
import requests   # ✅ missing import
from dotenv import load_dotenv
from botocore.config import Config
import uuid
from datetime import datetime
import tempfile
import glob

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# AWS Setup
# -----------------------
load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
    config=Config(read_timeout=300, connect_timeout=60)
)

# ----------------------------
# Upload small files (PDFs, slides)
# ----------------------------
def upload_pdf_to_s3(url, key_prefix="pdfs/"):
    """Stream PDF from URL and upload directly to S3."""
    filename = url.split("/")[-1]
    s3_key = f"{key_prefix}{filename}"

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            s3.upload_fileobj(r.raw, S3_BUCKET, s3_key)

        logger.info(f"Uploaded PDF → s3://{S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Failed to upload PDF {url}: {e}")
        return None

# -----------------------
# YouTube ingestion
# -----------------------

def upload_youtube_audio_with_chapters(video_url, key_prefix="audio/"):
    """Download YouTube audio to a temp dir, upload to S3, and save chapters + metadata."""
    video_id = video_url.split("v=")[-1]

    # Extract info first
    ydl_opts = {"skip_download": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
        ext = info.get("ext", "m4a")
        chapters = info.get("chapters", [])

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Let yt-dlp create the file inside tmp_dir
        cmd = [
            "yt-dlp",
            "-f", "140/251/bestaudio/best",
            "--no-overwrites",
            "--no-continue",
            "--no-part",
            "-o", os.path.join(tmp_dir, "%(id)s.%(ext)s"),
            video_url
        ]
        subprocess.run(cmd, check=True)

        # Find downloaded file
        downloaded_files = glob.glob(f"{tmp_dir}/*")
        if not downloaded_files:
            raise RuntimeError(f"No file downloaded for {video_url}")

        local_file = downloaded_files[0]
        ext = os.path.splitext(local_file)[-1].lstrip(".")
        audio_key = f"{key_prefix}{video_id}.{ext}"

        # Upload audio
        s3.upload_file(local_file, S3_BUCKET, audio_key)
        logger.info(f"Uploaded audio → s3://{S3_BUCKET}/{audio_key}")

        # Upload chapters
        chapters_key = None
        if chapters:
            chapters_key = f"{key_prefix}{video_id}_chapters.json"
            body = json.dumps(chapters, indent=2, ensure_ascii=False).encode("utf-8")
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=chapters_key,
                Body=body,
                ContentType="application/json"
            )
            logger.info(f"Uploaded chapters → s3://{S3_BUCKET}/{chapters_key}")
        else:
            logger.warning(f"No chapters found for {video_url}")

        return audio_key, chapters_key
# -----------------------
# Main Ingestion
# -----------------------
def run_ingestion(discovery_file):
    """Read discovery JSON and upload all resources to S3."""
    job_id = str(uuid.uuid4())
    start_time = datetime.utcnow()
    timestamp = start_time.isoformat()
    

    

    if discovery_file.startswith("s3://"):
        # Load discovery JSON from S3
        path = discovery_file[len("s3://"):]
        bucket, *key_parts = path.split("/")
        print(key_parts)
        key = '/'.join(key_parts)
        print(key,bucket)
        obj = s3.get_object(Bucket=bucket, Key=key)
        resources = json.loads(obj["Body"].read().decode("utf-8"))
    else:
        with open(discovery_file, "r") as f:
            resources = json.load(f)
    processed = []

    for r in resources:
        enriched = dict(r)  # copy original metadata
        try:
            if r["type"] == "pdf":
                continue
                # s3_key = upload_pdf_to_s3(r["url"], key_prefix="pdfs/")
                # if s3_key:
                #     enriched.update({
                #         "s3_key": s3_key,
                #         "uploaded_at": datetime.utcnow().isoformat(),
                #         "status": "success"
                #     })
                # else:
                #     enriched.update({"status": "failed", "reason": "upload returned None"})
            elif r["type"] == "video":
                audio_key, chapters_key = upload_youtube_audio_with_chapters(r["url"], key_prefix="audio/")
                if audio_key:
                    enriched.update({
                        "s3_key": audio_key,
                        "chapters_key": chapters_key,
                        "uploaded_at": datetime.utcnow().isoformat(),
                        "status": "success"
                    })
                else:
                    enriched.update({"status": "failed", "reason": "upload returned None"})
            else:
                enriched.update({"status": "skipped", "reason": "unknown type"})
        except Exception as e:
            enriched.update({"status": "failed", "reason": str(e)})

        processed.append(enriched)

    end_time = datetime.utcnow()
    duration_seconds = (end_time - start_time).total_seconds()
    # Save ingestion metadata
    ingestion_metadata = {
        "job_id": job_id,
        "ingested_at": timestamp,
        "num_total": len(resources),
        "num_success": sum(1 for r in processed if r["status"] == "success"),
        "num_failed": sum(1 for r in processed if r["status"] == "failed"),
        "resources": processed,
    }

    s3_key = f"metadata/ingestion/ingestion_{timestamp}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(ingestion_metadata, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )
    logger.info(f"Uploaded ingestion metadata → s3://{S3_BUCKET}/{s3_key}")

    # ✅ Final summary log with runtime
    logger.info(
        f"Ingestion job {job_id} completed in {duration_seconds:.1f}s: "
        f"{ingestion_metadata['num_total']} total, "
        f"{ingestion_metadata['num_success']} success, "
        f"{ingestion_metadata['num_failed']} failed."
    )


if __name__ == "__main__":
    # Example: pass latest discovery file (local or s3)
    run_ingestion("s3://multimodal-llm-edu-data/urls/discovered_20250924-202015.json")
