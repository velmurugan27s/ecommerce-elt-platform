FROM python:3.11-slim-bookworm

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    pyspark==3.5.8 \
    "boto3>=1.35,<2" \
    "pyarrow>=17,<20"

COPY spark/ /app/spark/

CMD ["python", "-m", "spark.jobs.process_bronze_to_silver"]
