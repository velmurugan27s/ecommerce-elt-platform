import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from ingestion.minio_client import (
    BUCKET_NAME,
    PROJECT_ROOT,
    ensure_bucket_exists,
    s3_client,
)

SOURCE_DIR = PROJECT_ROOT / "data" / "raw"
MANIFEST_DIR = PROJECT_ROOT / "data" / "processed" / "manifests"

DATASETS = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "category_translation",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA-256 without loading the entire file into memory."""
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def object_exists(object_key: str):
    """Return object metadata if present, otherwise None."""
    try:
        return s3_client.head_object(
            Bucket=BUCKET_NAME,
            Key=object_key,
        )

    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = error.response.get("Error", {}).get("Code")

        if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
            return None

        raise


def upload_source_file(file_path: Path, batch_id: str) -> dict:
    """Upload an immutable, checksum-addressed source object."""
    dataset = DATASETS[file_path.name]
    checksum = calculate_sha256(file_path)
    file_size = file_path.stat().st_size

    object_key = (
        f"bronze/olist/{dataset}/"
        f"sha256={checksum}/{file_path.name}"
    )

    existing = object_exists(object_key)

    if existing is not None:
        existing_checksum = existing.get("Metadata", {}).get("sha256")
        existing_size = existing.get("ContentLength")

        if existing_checksum != checksum or existing_size != file_size:
            raise RuntimeError(
                f"Existing object failed integrity validation: {object_key}"
            )

        logger.info("SKIPPED (already uploaded): %s", file_path.name)
        status = "skipped"

    else:
        s3_client.upload_file(
            str(file_path),
            BUCKET_NAME,
            object_key,
            ExtraArgs={
                "ContentType": "text/csv",
                "Metadata": {
                    "sha256": checksum,
                    "source_filename": file_path.name,
                    "batch_id": batch_id,
                },
            },
        )

        # Verify the object after upload.
        uploaded = s3_client.head_object(
            Bucket=BUCKET_NAME,
            Key=object_key,
        )

        if uploaded["ContentLength"] != file_size:
            raise RuntimeError(
                f"Uploaded file size mismatch: {file_path.name}"
            )

        if uploaded.get("Metadata", {}).get("sha256") != checksum:
            raise RuntimeError(
                f"Uploaded checksum metadata mismatch: {file_path.name}"
            )

        logger.info("UPLOADED AND VERIFIED: %s", file_path.name)
        status = "uploaded"

    return {
        "dataset": dataset,
        "source_filename": file_path.name,
        "object_key": object_key,
        "sha256": checksum,
        "size_bytes": file_size,
        "status": status,
    }


def main() -> None:
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(f"Source directory not found: {SOURCE_DIR}")

    missing = [
        filename
        for filename in DATASETS
        if not (SOURCE_DIR / filename).is_file()
    ]

    if missing:
        raise FileNotFoundError(
            f"Missing source datasets: {', '.join(missing)}"
        )

    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    files = [SOURCE_DIR / name for name in DATASETS]

    # Calculate all checksums first, producing a deterministic batch ID.
    checksums = {
        file_path.name: calculate_sha256(file_path)
        for file_path in files
    }

    batch_content = "\n".join(
        f"{name}:{checksums[name]}"
        for name in sorted(checksums)
    )

    batch_id = hashlib.sha256(
        batch_content.encode("utf-8")
    ).hexdigest()[:20]

    logger.info("Starting batch: %s", batch_id)
    logger.info("Source datasets: %s", len(files))

    ensure_bucket_exists()

    results = []

    for file_path in files:
        results.append(upload_source_file(file_path, batch_id))

    manifest = {
        "batch_id": batch_id,
        "source": "Olist Brazilian E-Commerce Dataset",
        "bucket": BUCKET_NAME,
        "ingestion_timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "completed",
        "file_count": len(results),
        "total_size_bytes": sum(
            item["size_bytes"] for item in results
        ),
        "files": results,
    }

    manifest_path = MANIFEST_DIR / f"{batch_id}.json"

    with manifest_path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)

    manifest_key = f"metadata/manifests/{batch_id}.json"

    existing_manifest = object_exists(manifest_key)

    if existing_manifest is None:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Manifest uploaded: %s", manifest_key)
    else:
        logger.info("Manifest already exists: %s", manifest_key)

    logger.info("====================================")
    logger.info("BRONZE INGESTION COMPLETED")
    logger.info("Batch ID: %s", batch_id)
    logger.info("Files processed: %s", len(results))
    logger.info(
        "Total source size: %.2f MB",
        manifest["total_size_bytes"] / (1024 * 1024),
    )
    logger.info("Local manifest: %s", manifest_path)
    logger.info("====================================")
    print(f"BATCH_ID={batch_id}", flush=True)


if __name__ == "__main__":
    main()
