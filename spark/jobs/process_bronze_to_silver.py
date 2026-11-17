import csv
import argparse
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = os.getenv("MINIO_BUCKET", "olist-bronze")
ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

MANIFEST_PREFIX = "metadata/manifests/"
SILVER_PREFIX = "silver/olist/"
QUARANTINE_PREFIX = "quarantine/olist/"
REPORT_PREFIX = "metadata/silver-reports/"

EXPECTED_DATASETS = [
    "customers",
    "geolocation",
    "order_items",
    "payments",
    "reviews",
    "orders",
    "products",
    "sellers",
    "category_translation",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# DATASET DEFINITIONS
# ============================================================

DATASETS = {
    "customers": {
        "filename": "olist_customers_dataset.csv",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "keys": ["customer_id"],
        "types": {},
    },

    "geolocation": {
        "filename": "olist_geolocation_dataset.csv",
        "columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
        "keys": None,
        "types": {
            "geolocation_lat": "double",
            "geolocation_lng": "double",
        },
    },

    "order_items": {
        "filename": "olist_order_items_dataset.csv",
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
        "keys": ["order_id", "order_item_id"],
        "types": {
            "order_item_id": "integer",
            "shipping_limit_date": "timestamp",
            "price": "decimal(12,2)",
            "freight_value": "decimal(12,2)",
        },
    },

    "payments": {
        "filename": "olist_order_payments_dataset.csv",
        "columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
        "keys": ["order_id", "payment_sequential"],
        "types": {
            "payment_sequential": "integer",
            "payment_installments": "integer",
            "payment_value": "decimal(12,2)",
        },
    },

    "reviews": {
        "filename": "olist_order_reviews_dataset.csv",
        "columns": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
        "keys": ["review_id", "order_id"],
        "types": {
            "review_score": "integer",
            "review_creation_date": "timestamp",
            "review_answer_timestamp": "timestamp",
        },
    },

    "orders": {
        "filename": "olist_orders_dataset.csv",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "keys": ["order_id"],
        "types": {
            "order_purchase_timestamp": "timestamp",
            "order_approved_at": "timestamp",
            "order_delivered_carrier_date": "timestamp",
            "order_delivered_customer_date": "timestamp",
            "order_estimated_delivery_date": "timestamp",
        },
    },

    "products": {
        "filename": "olist_products_dataset.csv",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "keys": ["product_id"],
        "types": {
            "product_name_lenght": "integer",
            "product_description_lenght": "integer",
            "product_photos_qty": "integer",
            "product_weight_g": "integer",
            "product_length_cm": "integer",
            "product_height_cm": "integer",
            "product_width_cm": "integer",
        },
    },

    "sellers": {
        "filename": "olist_sellers_dataset.csv",
        "columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
        "keys": ["seller_id"],
        "types": {},
    },

    "category_translation": {
        "filename": "product_category_name_translation.csv",
        "columns": [
            "product_category_name",
            "product_category_name_english",
        ],
        "keys": ["product_category_name"],
        "types": {},
    },
}


# ============================================================
# S3 / SILO CLIENT
# ============================================================

if not ACCESS_KEY or not SECRET_KEY:
    raise RuntimeError(
        "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are required."
    )

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


# ============================================================
# MANIFEST DISCOVERY
# ============================================================

def get_completed_manifest(batch_id=None):
    if batch_id:
        manifest_key = f"{MANIFEST_PREFIX}{batch_id}.json"

        try:
            response = s3.get_object(
                Bucket=BUCKET,
                Key=manifest_key,
            )
        except s3.exceptions.NoSuchKey:
            raise RuntimeError(
                f"Bronze manifest not found for batch: {batch_id}"
            )

        manifest = json.loads(
            response["Body"].read().decode("utf-8")
        )

        if manifest.get("status") != "completed":
            raise RuntimeError(
                f"Bronze batch is not completed: {batch_id}"
            )

        if manifest.get("batch_id") != batch_id:
            raise RuntimeError(
                "Manifest batch ID does not match requested batch ID."
            )

        logger.info("Selected Bronze batch: %s", batch_id)
        return manifest

    paginator = s3.get_paginator("list_objects_v2")
    manifest_objects = []

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=MANIFEST_PREFIX,
    ):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".json"):
                manifest_objects.append(item)

    if not manifest_objects:
        raise RuntimeError("No Bronze manifests found.")

    completed_manifests = []

    for item in manifest_objects:
        response = s3.get_object(
            Bucket=BUCKET,
            Key=item["Key"],
        )

        manifest = json.loads(
            response["Body"].read().decode("utf-8")
        )

        if manifest.get("status") == "completed":
            completed_manifests.append(manifest)

    if not completed_manifests:
        raise RuntimeError("No completed Bronze manifest found.")

    completed_manifests.sort(
        key=lambda x: x.get("ingestion_timestamp_utc", "")
    )

    manifest = completed_manifests[-1]

    logger.info(
        "Selected latest completed Bronze batch: %s",
        manifest["batch_id"],
    )

    return manifest



def validate_manifest(manifest):
    entries = {
        item["dataset"]: item
        for item in manifest.get("files", [])
    }

    missing = set(EXPECTED_DATASETS) - set(entries)

    if missing:
        raise RuntimeError(
            f"Missing datasets in manifest: {sorted(missing)}"
        )

    for dataset in EXPECTED_DATASETS:
        if not entries[dataset].get("object_key"):
            raise RuntimeError(
                f"Missing object key for {dataset}"
            )

    return entries


# ============================================================
# SPARK SCHEMA AND TRANSFORMATIONS
# ============================================================

def make_source_schema(columns):
    return StructType([
        StructField(column, StringType(), True)
        for column in columns
    ])


def cast_columns(df, type_map):
    for column, data_type in type_map.items():
        df = df.withColumn(
            column,
            F.trim(F.col(column)).cast(data_type),
        )

    return df


def standardize_columns(df, dataset):
    if dataset == "products":
        df = df.withColumnRenamed(
            "product_name_lenght",
            "product_name_length",
        )

        df = df.withColumnRenamed(
            "product_description_lenght",
            "product_description_length",
        )

    return df


# ============================================================
# S3 OPERATIONS
# ============================================================

def download_bronze_object(object_key, local_path):
    logger.info("Downloading Bronze object: %s", object_key)

    s3.download_file(
        BUCKET,
        object_key,
        str(local_path),
    )


def delete_s3_prefix(prefix):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
    ):
        objects = [
            {"Key": item["Key"]}
            for item in page.get("Contents", [])
        ]

        if objects:
            s3.delete_objects(
                Bucket=BUCKET,
                Delete={
                    "Objects": objects,
                    "Quiet": True,
                },
            )


def upload_directory(local_directory, destination_prefix):
    local_directory = Path(local_directory)
    uploaded_count = 0

    for file_path in local_directory.rglob("*"):
        if not file_path.is_file():
            continue

        if file_path.name.startswith("_"):
            continue

        if file_path.name.startswith("."):
            continue

        relative_path = file_path.relative_to(local_directory)

        object_key = (
            destination_prefix.rstrip("/")
            + "/"
            + relative_path.as_posix()
        )

        s3.upload_file(
            str(file_path),
            BUCKET,
            object_key,
        )

        uploaded_count += 1

    return uploaded_count


# ============================================================
# REVIEWS CSV NORMALIZATION
# ============================================================

def normalize_reviews_csv(source_path, output_path, expected_columns):
    """
    Parse the original reviews CSV with Python's CSV parser.

    Re-write every field with standard CSV quoting.
    Embedded newlines and quotation marks are preserved.
    """

    normalized_count = 0

    with (
        Path(source_path).open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as source_file,
        Path(output_path).open(
            "w",
            encoding="utf-8",
            newline="",
        ) as output_file,
    ):
        reader = csv.reader(
            source_file,
            strict=True,
        )

        writer = csv.writer(
            output_file,
            quoting=csv.QUOTE_ALL,
            doublequote=True,
            lineterminator="\n",
        )

        header = next(reader)

        if header != expected_columns:
            raise ValueError(
                f"Unexpected reviews header: {header}"
            )

        writer.writerow(header)

        for record_number, row in enumerate(reader, start=2):
            if len(row) != len(header):
                raise ValueError(
                    f"Malformed reviews record {record_number}: "
                    f"expected {len(header)} columns, "
                    f"found {len(row)}"
                )

            writer.writerow(row)
            normalized_count += 1

    logger.info(
        "Reviews CSV normalized: %s logical records",
        normalized_count,
    )

    return normalized_count


# ============================================================
# PROCESS ONE DATASET
# ============================================================

def process_dataset(
    spark,
    dataset,
    config,
    manifest_entry,
    batch_id,
    workdir,
):
    filename = config["filename"]

    local_csv = Path(workdir) / filename

    silver_dir = (
        Path(workdir) / "silver" / dataset
    )

    quarantine_dir = (
        Path(workdir) / "quarantine" / dataset
    )

    download_bronze_object(
        manifest_entry["object_key"],
        local_csv,
    )

    # --------------------------------------------------------
    # Normalize reviews before Spark reads the file.
    # --------------------------------------------------------

    spark_input_path = local_csv

    if dataset == "reviews":
        normalized_csv = (
            Path(workdir) / "reviews_normalized.csv"
        )

        normalize_reviews_csv(
            source_path=local_csv,
            output_path=normalized_csv,
            expected_columns=config["columns"],
        )

        spark_input_path = normalized_csv

    # --------------------------------------------------------
    # Read CSV with Spark.
    # --------------------------------------------------------

    schema = make_source_schema(config["columns"])

    reader = (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .option("multiLine", True)
    )

    if dataset == "reviews":
        reader = (
            reader
            .option("quote", '"')
            .option("escape", '"')
        )

    source_df = (
        reader
        .schema(schema)
        .csv(str(spark_input_path))
    )

    input_count = source_df.count()

    # --------------------------------------------------------
    # Cast and standardize columns.
    # --------------------------------------------------------

    typed_df = cast_columns(
        source_df,
        config["types"],
    )

    typed_df = standardize_columns(
        typed_df,
        dataset,
    )

    # --------------------------------------------------------
    # Required key validation.
    # --------------------------------------------------------

    required_keys = config["keys"]

    if required_keys is None:
        required_keys = config["columns"][:1]

    missing_key_conditions = [
        F.when(
            F.col(key).isNull()
            | (
                F.trim(
                    F.col(key).cast("string")
                ) == ""
            ),
            F.lit(f"missing_{key}"),
        )
        for key in required_keys
    ]

    typed_df = (
        typed_df
        .withColumn(
            "_dq_rejection_reason",
            F.concat_ws(
                "; ",
                *missing_key_conditions,
            ),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "_processed_at_utc",
            F.current_timestamp(),
        )
    )

    # --------------------------------------------------------
    # Separate invalid records before deduplication.
    # --------------------------------------------------------

    rejected_df = typed_df.filter(
        F.col("_dq_rejection_reason") != ""
    )

    rejected_count = rejected_df.count()

    valid_before_dedup_df = (
        typed_df
        .filter(
            F.col("_dq_rejection_reason") == ""
        )
        .drop("_dq_rejection_reason")
    )

    valid_before_dedup_count = (
        valid_before_dedup_df.count()
    )

    # --------------------------------------------------------
    # Deduplicate using investigated dataset grain.
    # --------------------------------------------------------

    if config["keys"] is None:
        valid_df = valid_before_dedup_df.dropDuplicates()
    else:
        valid_df = valid_before_dedup_df.dropDuplicates(
            config["keys"]
        )

    valid_count = valid_df.count()

    duplicate_count = (
        valid_before_dedup_count - valid_count
    )

    # --------------------------------------------------------
    # Write local Parquet files.
    # --------------------------------------------------------

    valid_df.write.mode("overwrite").parquet(
        str(silver_dir)
    )

    if rejected_count > 0:
        rejected_df.write.mode("overwrite").parquet(
            str(quarantine_dir)
        )

    # --------------------------------------------------------
    # Upload Silver and quarantine outputs.
    # --------------------------------------------------------

    silver_object_prefix = (
        f"{SILVER_PREFIX}"
        f"{dataset}/batch_id={batch_id}/"
    )

    quarantine_object_prefix = (
        f"{QUARANTINE_PREFIX}"
        f"{dataset}/batch_id={batch_id}/"
    )

    # Clear only the outputs for this batch.
    # Bronze objects and PostgreSQL data are untouched.

    delete_s3_prefix(silver_object_prefix)
    delete_s3_prefix(quarantine_object_prefix)

    silver_files = upload_directory(
        silver_dir,
        silver_object_prefix,
    )

    quarantine_files = 0

    if rejected_count > 0:
        quarantine_files = upload_directory(
            quarantine_dir,
            quarantine_object_prefix,
        )

    logger.info(
        "%s: input=%s duplicates=%s valid=%s rejected=%s",
        dataset,
        input_count,
        duplicate_count,
        valid_count,
        rejected_count,
    )

    return {
        "dataset": dataset,
        "input_rows": input_count,
        "duplicates_removed": duplicate_count,
        "valid_rows": valid_count,
        "rejected_rows": rejected_count,
        "silver_files_uploaded": silver_files,
        "quarantine_files_uploaded": quarantine_files,
        "silver_prefix": silver_object_prefix,
        "quarantine_prefix": (
            quarantine_object_prefix
            if rejected_count > 0
            else None
        ),
    }


# ============================================================
# QUALITY REPORT
# ============================================================

def create_quality_report(batch_id, results):
    total_input = sum(
        item["input_rows"]
        for item in results
    )

    total_valid = sum(
        item["valid_rows"]
        for item in results
    )

    total_duplicates = sum(
        item["duplicates_removed"]
        for item in results
    )

    total_rejected = sum(
        item["rejected_rows"]
        for item in results
    )

    return {
        "batch_id": batch_id,
        "layer": "silver",
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "datasets_processed": len(results),
        "totals": {
            "input_rows": total_input,
            "valid_rows": total_valid,
            "duplicates_removed": total_duplicates,
            "rejected_rows": total_rejected,
        },
        "datasets": results,
    }


def upload_quality_report(report, batch_id):
    report_key = (
        f"{REPORT_PREFIX}{batch_id}.json"
    )

    report_body = json.dumps(
        report,
        indent=2,
        default=str,
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=report_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Quality report: %s",
        report_key,
    )


# ============================================================
# MAIN
# ============================================================

def main(batch_id=None):
    spark = (
        SparkSession.builder
        .appName("Olist-Bronze-to-Silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        manifest = get_completed_manifest(batch_id)

        batch_id = manifest["batch_id"]

        manifest_entries = validate_manifest(manifest)

        results = []

        with tempfile.TemporaryDirectory(
            prefix="olist_silver_"
        ) as workdir:

            for dataset in EXPECTED_DATASETS:
                result = process_dataset(
                    spark=spark,
                    dataset=dataset,
                    config=DATASETS[dataset],
                    manifest_entry=manifest_entries[dataset],
                    batch_id=batch_id,
                    workdir=workdir,
                )

                results.append(result)

            report = create_quality_report(
                batch_id,
                results,
            )

            upload_quality_report(
                report,
                batch_id,
            )

        totals = report["totals"]

        logger.info("====================================")
        logger.info("SILVER PROCESSING COMPLETED")
        logger.info("Batch ID: %s", batch_id)
        logger.info(
            "Datasets processed: %s",
            report["datasets_processed"],
        )
        logger.info("Input rows: %s", totals["input_rows"])
        logger.info("Valid rows: %s", totals["valid_rows"])
        logger.info(
            "Duplicates removed: %s",
            totals["duplicates_removed"],
        )
        logger.info("Rejected rows: %s", totals["rejected_rows"])
        logger.info("====================================")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process Bronze CSV data into Silver Parquet"
    )

    parser.add_argument(
        "--batch-id",
        required=False,
        help="Specific completed Bronze batch to process",
    )

    args = parser.parse_args()
    main(batch_id=args.batch_id)