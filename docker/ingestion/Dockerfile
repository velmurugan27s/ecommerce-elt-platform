FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/elt

RUN pip install --no-cache-dir \
    boto3==1.44.4 \
    python-dotenv==1.2.4 \
    pyarrow==26.0.1 \
    psycopg2-binary==2.9.13

COPY ingestion/ /opt/elt/ingestion/

CMD ["python", "--version"]3