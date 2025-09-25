"""Pipeline for converting PDF files from S3 into text chunks and uploading them back to S3."""

import json
import os
import tempfile
from pathlib import Path
from typing import Iterator, List, Optional, Sequence

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langsmith import Client
from PyPDF2 import PdfReader

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
PDF_PREFIX = os.getenv("PDF_PREFIX", "")
OUTPUT_PREFIX = os.getenv("OUTPUT_CHUNKS_PREFIX", "chunks/")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "200"))
LANGSMITH_DATASET = os.getenv("LANGSMITH_DATASET", "s3-pdf-chunks")

S3_CONFIG = Config(
    read_timeout=300,
    connect_timeout=60,
    retries={"max_attempts": 5, "mode": "standard"},
)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
    config=S3_CONFIG,
)

langsmith_client: Optional[Client] = None
langsmith_dataset_id: Optional[str] = None

if os.getenv("LANGSMITH_API_KEY"):
    try:
        langsmith_client = Client()
    except Exception as error:  # pragma: no cover - optional telemetry
        print(f"LangSmith disabled: failed to initialise client ({error})", flush=True)
        langsmith_client = None


def iter_pdf_keys(bucket: str, prefix: str = "") -> Iterator[str]:
    """Yield PDF object keys within ``bucket`` under ``prefix`` if provided."""
    continuation_token = None

    while True:
        request = {"Bucket": bucket}
        if prefix:
            request["Prefix"] = prefix
        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**request)

        for entry in response.get("Contents", []):
            key = entry.get("Key", "")
            if key.lower().endswith(".pdf"):
                yield key

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")


def download_pdf(bucket: str, key: str, destination_dir: str) -> Path:
    """Download a PDF object to ``destination_dir`` and return the local path."""
    target_path = Path(destination_dir) / Path(key).name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket, key, str(target_path))
    return target_path


def pdf_to_text(pdf_path: Path) -> str:
    """Extract plain text from a PDF using PyPDF2."""
    reader = PdfReader(str(pdf_path))
    pages: List[str] = []

    for page in reader.pages:
        extracted = page.extract_text() or ""
        pages.append(extracted.strip())

    return "\n\n".join(part for part in pages if part)


def build_output_key(prefix: str, source_key: str) -> str:
    """Construct an output object key that mirrors the source name."""
    base_name = Path(source_key).stem + ".json"
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        return base_name
    return f"{normalized_prefix}/{base_name}"


def ensure_langsmith_dataset() -> Optional[str]:
    """Fetch or create the LangSmith dataset that stores chunk metadata."""
    global langsmith_dataset_id

    if not langsmith_client:
        return None

    if langsmith_dataset_id:
        return langsmith_dataset_id

    try:
        dataset = langsmith_client.read_dataset(name=LANGSMITH_DATASET)
    except Exception:
        dataset = langsmith_client.create_dataset(
            name=LANGSMITH_DATASET,
            description="Chunks derived from S3-hosted PDF files.",
        )

    langsmith_dataset_id = dataset.id
    return langsmith_dataset_id


def log_chunks_to_langsmith(
    source_key: str,
    output_key: str,
    chunks: Sequence[str],
) -> None:
    """Store metadata about generated chunks inside LangSmith (best-effort)."""
    if not langsmith_client:
        return

    dataset_id = ensure_langsmith_dataset()
    if not dataset_id:
        return

    try:
        langsmith_client.create_example(
            dataset_id=dataset_id,
            inputs={"s3_pdf_key": source_key},
            outputs={
                "s3_chunk_key": output_key,
                "chunks": list(chunks),
            },
            metadata={"num_chunks": len(chunks)},
        )
    except Exception as error:  # pragma: no cover - optional telemetry
        print(
            f"Warning: failed to log LangSmith example for {source_key}: {error}",
            flush=True,
        )


def upload_chunks(
    bucket: str,
    output_key: str,
    source_key: str,
    chunks: Sequence[str],
    chunk_size: int,
    chunk_overlap: int,
) -> None:
    """Upload JSON payload describing the chunked text to S3."""
    payload = {
        "source": source_key,
        "chunk_size": chunk_size,
        "chunk_overlap": chunk_overlap,
        "num_chunks": len(chunks),
        "chunks": [
            {"index": index, "text": chunk}
            for index, chunk in enumerate(chunks)
        ],
    }

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=body,
        ContentType="application/json",
    )

    log_chunks_to_langsmith(source_key, output_key, chunks)


def main() -> None:
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET environment variable is required.")

    if not AWS_REGION:
        raise RuntimeError("AWS_DEFAULT_REGION environment variable is required.")

    print(
        f"Processing PDFs from s3://{S3_BUCKET}/{PDF_PREFIX} -> {OUTPUT_PREFIX}",
        flush=True,
    )

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
    )

    processed = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        for key in iter_pdf_keys(S3_BUCKET, PDF_PREFIX):
            print(f"Processing {key}", flush=True)
            try:
                local_pdf = download_pdf(S3_BUCKET, key, temp_dir)
                text = pdf_to_text(local_pdf)

                if not text.strip():
                    print(f"Skipping {key}: extracted text is empty", flush=True)
                    continue

                chunks = splitter.split_text(text)

                if not chunks:
                    print(f"Skipping {key}: splitter produced no chunks", flush=True)
                    continue

                output_key = build_output_key(OUTPUT_PREFIX, key)
                upload_chunks(
                    S3_BUCKET,
                    output_key,
                    key,
                    chunks,
                    CHUNK_SIZE,
                    CHUNK_OVERLAP,
                )
                print(
                    f"Uploaded {len(chunks)} chunks to s3://{S3_BUCKET}/{output_key}",
                    flush=True,
                )
                processed += 1
            except Exception as error:
                print(f"Failed to process {key}: {error}", flush=True)

    print(f"Completed processing for {processed} PDF file(s).", flush=True)


if __name__ == "__main__":
    main()
