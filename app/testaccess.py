import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

# Test listing objects in the bucket
try:
    print("Testing access to S3 bucket...")
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    if "Contents" in response:
        print(f"Bucket '{S3_BUCKET}' is accessible. Found {len(response['Contents'])} objects.")
    else:
        print(f"Bucket '{S3_BUCKET}' is accessible but empty.")
except Exception as e:
    print(f"Error accessing bucket '{S3_BUCKET}': {e}")
