
import argparse
import io
import logging
import os
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import psycopg2
from dotenv import load_dotenv
from botocore.config import Config


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

BATCH_SIZE = 50_000
SILVER_PREFIX = "silver/olist/"

DATASETS = {
    "customers": {
        "table": "staging.stg_customers",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "geolocation": {
        "table": "staging.stg_geolocation",
        "columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "order_items": {
        "table": "staging.stg_order_items",
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "payments": {
        "table": "staging.stg_payments",
        "columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "reviews": {
        "table": "staging.stg_reviews",
        "columns": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "orders": {
        "table": "staging.stg_orders",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "products": {
        "table": "staging.stg_products",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "sellers": {
        "table": "staging.stg_sellers",
        "columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
    "category_translation": {
        "table": "staging.stg_category_translation",
        "columns": [
            "product_category_name",
            "product_category_name_english",
            "_batch_id",
            "_processed_at_utc",
        ],
    },
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# CONNECTIONS
# ============================================================

def create_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not all([endpoint, access_key, secret_key]):
        raise RuntimeError(
            "Missing Silo connection variables in .env"
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def create_postgres_connection():
    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "POSTGRES_PORT",
    ]

    missing = [
        key for key in required
        if not os.getenv(key)
    ]

    if missing:
        raise RuntimeError(
            f"Missing PostgreSQL environment variables: {missing}"
        )

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


# ============================================================
# S3 OBJECT DISCOVERY
# ============================================================

def list_silver_objects(s3, bucket, dataset, batch_id):
    prefix = (
        f"{SILVER_PREFIX}"
        f"{dataset}/batch_id={batch_id}/"
    )

    paginator = s3.get_paginator("list_objects_v2")

    objects = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]

            if key.endswith(".parquet"):
                objects.append(key)

    return sorted(objects)


# ============================================================
# PARQUET TO POSTGRES CSV
# ============================================================

def csv_value(value):
    """
    Format one PostgreSQL CSV field.

    Non-null values are quoted and escaped.
    Null values use PostgreSQL's unquoted \\N marker.
    """

    if value is None:
        return r"\N"

    text = str(value)

    return '"' + text.replace('"', '""') + '"'


def copy_parquet_object(
    s3,
    bucket,
    object_key,
    cursor,
    table,
    columns,
):
    response = s3.get_object(
        Bucket=bucket,
        Key=object_key,
    )

    parquet_bytes = response["Body"].read()

    parquet_file = pq.ParquetFile(
        io.BytesIO(parquet_bytes)
    )

    parquet_columns = parquet_file.schema_arrow.names

    missing = set(columns) - set(parquet_columns)
    extra = set(parquet_columns) - set(columns)

    if missing or extra:
        raise ValueError(
            f"Parquet schema mismatch for {object_key}. "
            f"Missing={sorted(missing)}, "
            f"Extra={sorted(extra)}"
        )

    column_positions = [
        parquet_columns.index(column)
        for column in columns
    ]

    column_sql = ", ".join(columns)

    copy_sql = (
        f"COPY {table} ({column_sql}) "
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    )

    total_rows = 0

    for record_batch in parquet_file.iter_batches(
        batch_size=BATCH_SIZE,
        columns=columns,
    ):
        buffer = io.StringIO()

        arrays = [
            record_batch.column(index).to_pylist()
            for index in range(len(columns))
        ]

        for row in zip(*arrays):
            buffer.write(
                ",".join(csv_value(value) for value in row)
            )
            buffer.write("\n")

        buffer.seek(0)

        cursor.copy_expert(
            copy_sql,
            buffer,
        )

        total_rows += record_batch.num_rows

    logger.info(
        "Loaded %s rows from %s",
        total_rows,
        object_key,
    )

    return total_rows


# ============================================================
# DATASET LOADING
# ============================================================

def load_dataset(
    s3,
    bucket,
    cursor,
    dataset,
    batch_id,
):
    config = DATASETS[dataset]

    table = config["table"]
    columns = config["columns"]

    objects = list_silver_objects(
        s3,
        bucket,
        dataset,
        batch_id,
    )

    if not objects:
        raise RuntimeError(
            f"No Silver Parquet files found for {dataset}, "
            f"batch {batch_id}"
        )

    logger.info(
        "Loading dataset=%s files=%s",
        dataset,
        len(objects),
    )

    total_rows = 0

    for object_key in objects:
        total_rows += copy_parquet_object(
            s3=s3,
            bucket=bucket,
            object_key=object_key,
            cursor=cursor,
            table=table,
            columns=columns,
        )

    cursor.execute(
        f"SELECT COUNT(*) FROM {table} WHERE _batch_id = %s",
        (batch_id,),
    )

    database_rows = cursor.fetchone()[0]

    if database_rows != total_rows:
        raise RuntimeError(
            f"Row count mismatch for {dataset}: "
            f"Parquet={total_rows}, PostgreSQL={database_rows}"
        )

    logger.info(
        "VERIFIED %s: %s rows",
        dataset,
        database_rows,
    )

    return total_rows


# ============================================================
# MAIN PIPELINE
# ============================================================

def main(batch_id):
    bucket = os.getenv(
        "MINIO_BUCKET",
        "olist-bronze",
    )

    s3 = create_s3_client()
    connection = create_postgres_connection()

    results = {}

    try:
        with connection:
            with connection.cursor() as cursor:

                # Confirm every dataset has Parquet objects
                # before modifying staging data.

                for dataset in DATASETS:
                    objects = list_silver_objects(
                        s3,
                        bucket,
                        dataset,
                        batch_id,
                    )

                    if not objects:
                        raise RuntimeError(
                            f"Missing Silver data for {dataset}"
                        )

                logger.info(
                    "Starting PostgreSQL load for batch %s",
                    batch_id,
                )

                # Replace only this batch's existing records.
                # Other batches remain untouched.

                for config in DATASETS.values():
                    cursor.execute(
                        f"DELETE FROM {config['table']} "
                        "WHERE _batch_id = %s",
                        (batch_id,),
                    )

                # Load all nine datasets within one transaction.

                for dataset in DATASETS:
                    results[dataset] = load_dataset(
                        s3=s3,
                        bucket=bucket,
                        cursor=cursor,
                        dataset=dataset,
                        batch_id=batch_id,
                    )

        # The transaction commits only after all datasets
        # have passed their row-count checks.

        logger.info("=" * 55)
        logger.info("POSTGRESQL LOAD COMPLETED")
        logger.info("Batch ID: %s", batch_id)

        for dataset, count in results.items():
            logger.info(
                "%-25s %s rows",
                dataset,
                count,
            )

        logger.info(
            "TOTAL ROWS LOADED: %s",
            sum(results.values()),
        )

    except Exception:
        connection.rollback()
        logger.exception(
            "PostgreSQL load failed. Transaction rolled back."
        )
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Silver Parquet data into PostgreSQL"
    )

    parser.add_argument(
        "--batch-id",
        required=True,
        help="Silver processing batch ID",
    )

    args = parser.parse_args()

    main(args.batch_id)
