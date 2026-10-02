"""
Upload one Synthea CSV into the S3 raw zone, byte-for-byte untouched.

Run it inside the Airflow container (that image has boto3 installed):

    docker compose exec airflow-scheduler \
        python /opt/airflow/include/ingest/upload_raw.py patients
"""
import os
import sys
from datetime import date
from pathlib import Path

import boto3

# Where the CSVs live INSIDE the container.
# Host ./synthea/output is mounted here by docker-compose.yaml.
DATA_DIR = Path("/opt/airflow/data/synthea/csv")


# Raw files are partitioned by table and ingestion date in the S3 key so
# historical backfills are easy to replay and inspect.
def build_key(table: str, filename: str, ingestion_date: str) -> str:
    """Pure string building."""
    return f"raw/{table}/ingestion_date={ingestion_date}/{filename}"


# Upload one CSV file to S3. The DAG will call this function for each table.
def upload_one_file(table: str, ingestion_date: str) -> dict:
    bucket = os.environ["S3_BUCKET"]          # KeyError = loud fail if unset

    local_path = DATA_DIR / f"{table}.csv"
    if not local_path.exists():
        raise FileNotFoundError(f"No such source file: {local_path}")

    key = build_key(table, local_path.name, ingestion_date)

    # No credentials passed on purpose. boto3 picks up AWS_ACCESS_KEY_ID,
    # AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION from the environment
    # (compose injects them from ../.env).
    s3 = boto3.client("s3")

    # upload_file streams from disk and auto-switches to multipart for big
    # files. Do NOT use put_object here: it loads the whole file into RAM,
    # which will choke on claims_transactions.csv (~410 MB).
    s3.upload_file(str(local_path), bucket, key)

    size = local_path.stat().st_size
    print(f"uploaded s3://{bucket}/{key} ({size:,} bytes)")
    return {"table": table, "bucket": bucket, "key": key, "bytes": size}


if __name__ == "__main__":
    # Manual runs default to today. The DAG will pass Airflow's logical date
    # ("{{ ds }}") instead, so backfills stay reproducible.
    table_arg = sys.argv[1]
    date_arg = sys.argv[2] if len(sys.argv) > 2 else date.today().isoformat()
    upload_one_file(table_arg, date_arg)