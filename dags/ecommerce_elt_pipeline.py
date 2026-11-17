import os
import re
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# ---------------------------------------------------------
# PROJECT CONFIGURATION
# ---------------------------------------------------------

PROJECT_ROOT = "/home/vel/projects/ecommerce-elt-platform"
DOCKER_NETWORK = "ecommerce-elt-platform_default"

INGESTION_IMAGE = "ecommerce-elt-platform-ingestion:latest"
SPARK_IMAGE = "ecommerce-elt-platform-spark:latest"
DBT_IMAGE = "ecommerce-elt-platform-dbt:latest"


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name} is missing."
        )
    return value


# Silo endpoint must use the Docker service hostname,
# not localhost, because the task containers share the ELT network.
SILO_ENV = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ROOT_USER": required_env("MINIO_ROOT_USER"),
    "MINIO_ROOT_PASSWORD": required_env("MINIO_ROOT_PASSWORD"),
    "MINIO_BUCKET": required_env("MINIO_BUCKET"),
}

# Ingestion container needs both Silo and PostgreSQL.
INGESTION_ENV = {
    **SILO_ENV,
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": required_env("POSTGRES_USER"),
    "POSTGRES_PASSWORD": required_env("POSTGRES_PASSWORD"),
    "POSTGRES_DB": required_env("POSTGRES_DB"),
}

# dbt connects directly to the warehouse PostgreSQL container.
DBT_ENV = {
    "DBT_PROFILES_DIR": "/usr/app/dbt",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    "POSTGRES_USER": required_env("POSTGRES_USER"),
    "POSTGRES_PASSWORD": required_env("POSTGRES_PASSWORD"),
    "POSTGRES_DB": required_env("POSTGRES_DB"),
}

PROJECT_MOUNT = Mount(
    source=PROJECT_ROOT,
    target="/opt/elt",
    type="bind",
)

DBT_MOUNT = Mount(
    source=f"{PROJECT_ROOT}/dbt",
    target="/usr/app/dbt",
    type="bind",
)


# All Docker tasks use the host Docker socket.
DOCKER_COMMON = {
    "docker_url": "unix://var/run/docker.sock",
    "docker_conn_id": None,
    "network_mode": DOCKER_NETWORK,
    "mount_tmp_dir": False,
    "auto_remove": "success",
}


# ---------------------------------------------------------
# BATCH ID VALIDATION
# ---------------------------------------------------------

@task
def extract_batch_id(ingestion_output):
    """
    Extract and validate the BATCH_ID printed by Bronze ingestion.
    """

    if isinstance(ingestion_output, bytes):
        ingestion_output = ingestion_output.decode("utf-8")

    if isinstance(ingestion_output, list):
        ingestion_output = "\n".join(
            str(line) for line in ingestion_output
        )

    if not isinstance(ingestion_output, str):
        raise ValueError(
            f"Unexpected ingestion output type: "
            f"{type(ingestion_output).__name__}"
        )

    matches = re.findall(
        r"BATCH_ID=([0-9a-f]{20})",
        ingestion_output
    )

    if not matches:
        raise ValueError(
            "Could not find a valid BATCH_ID in Bronze ingestion output."
        )

    batch_id = matches[-1]

    print(f"Validated batch ID: {batch_id}")

    return batch_id


# ---------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------

with DAG(
    dag_id="ecommerce_elt_pipeline",
    description="End-to-end Olist E-Commerce ELT pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["ecommerce", "elt", "spark", "dbt", "postgres"],
) as dag:

    # -----------------------------------------------------
    # 1. BRONZE: CSV -> Silo
    # -----------------------------------------------------

    ingest_bronze = DockerOperator(
        task_id="ingest_bronze",
        image=INGESTION_IMAGE,
        command=[
            "python",
            "-m",
            "ingestion.upload_to_bronze",
        ],
        working_dir="/opt/elt",
        mounts=[PROJECT_MOUNT],
        environment=INGESTION_ENV,
        **DOCKER_COMMON,
    )

    # -----------------------------------------------------
    # 2. EXTRACT BATCH ID FROM BRONZE OUTPUT
    # -----------------------------------------------------

    batch_id = extract_batch_id(ingest_bronze.output)

    # -----------------------------------------------------
    # 3. SILVER: Bronze -> Spark -> Parquet
    # -----------------------------------------------------

    process_silver = DockerOperator(
        task_id="process_silver",
        image=SPARK_IMAGE,
        command=[
            "python",
            "-m",
            "spark.jobs.process_bronze_to_silver",
            "--batch-id",
            "{{ ti.xcom_pull(task_ids='extract_batch_id') }}",
        ],
        environment=SILO_ENV,
        **DOCKER_COMMON,
    )

    # -----------------------------------------------------
    # 4. POSTGRESQL: Silver -> Staging
    # -----------------------------------------------------

    load_postgres = DockerOperator(
        task_id="load_postgres",
        image=INGESTION_IMAGE,
        command=[
            "python",
            "-m",
            "ingestion.load_silver_to_postgres",
            "--batch-id",
            "{{ ti.xcom_pull(task_ids='extract_batch_id') }}",
        ],
        working_dir="/opt/elt",
        mounts=[PROJECT_MOUNT],
        environment=INGESTION_ENV,
        **DOCKER_COMMON,
    )

    # -----------------------------------------------------
    # 5. DBT RUN: Staging -> Dimensional Models
    # -----------------------------------------------------

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        command=[
            "run",
            "--vars",
            "batch_id: {{ ti.xcom_pull(task_ids='extract_batch_id') }}",
        ],
        working_dir="/usr/app/dbt",
        mounts=[DBT_MOUNT],
        environment=DBT_ENV,
        **DOCKER_COMMON,
    )

    # -----------------------------------------------------
    # 6. DBT TEST: Validate Models
    # -----------------------------------------------------

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        command=[
            "test",
            "--vars",
            "batch_id: {{ ti.xcom_pull(task_ids='extract_batch_id') }}",
        ],
        working_dir="/usr/app/dbt",
        mounts=[DBT_MOUNT],
        environment=DBT_ENV,
        **DOCKER_COMMON,
    )

    # -----------------------------------------------------
    # PIPELINE DEPENDENCIES
    # -----------------------------------------------------

    batch_id >> process_silver
    process_silver >> load_postgres
    load_postgres >> dbt_run
    dbt_run >> dbt_test
