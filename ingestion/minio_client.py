import os
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

ENDPOINT_URL = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "olist-bronze")

if not ACCESS_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Missing MINIO_ROOT_USER or MINIO_ROOT_PASSWORD in project .env"
    )

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


def ensure_bucket_exists() -> None:
    """Create the Bronze bucket if it does not already exist."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket already exists: {BUCKET_NAME}")

    except Exception as error:
        from botocore.exceptions import ClientError

        if not isinstance(error, ClientError):
            raise

        response = error.response
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = response.get("Error", {}).get("Code")

        if status == 404 or code in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"Created Bronze bucket: {BUCKET_NAME}")
        else:
            raise RuntimeError(
                f"Could not verify Bronze bucket: {response.get('Error')}"
            ) from error
