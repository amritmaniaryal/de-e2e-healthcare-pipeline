"""
extract_raw_data — land the Synthea claims CSVs into the S3 raw/ zone, untouched.

Manual trigger only for now. Airflow fans out one mapped task per table, so a
single failure retries on its own instead of re-uploading everything.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from ingest.upload_raw import upload_one_file

# Fixed and timezone-aware on purpose. Never datetime.now() here: start_date is
# re-evaluated on every parse (~30s), so a dynamic value would drift forever.
START_DATE = pendulum.datetime(2026, 10, 1, tz="UTC")

# Our claims-relevant subset (11 of the 18 Synthea exports).
TABLES = [
    "patients",
    "providers",
    "organizations",
    "payers",
    "payer_transitions",
    "conditions",
    "medications",
    "encounters",
    "procedures",
    "claims",
    "claims_transactions",
]



def alert_on_failure(context):
    """Alerting stub — swap for Slack/email in Week 5."""
    ti = context.get("task_instance")
    print(f"FAILED: {ti.dag_id}.{ti.task_id} (run_id={ti.run_id})")


@dag(
    dag_id="extract_raw_data",
    schedule=None,          # manual while developing; "@daily" once it's green
    start_date=START_DATE,
    catchup=False,          # never auto-backfill a backlog of intervals
    max_active_tasks=4,     # don't fire 11 uploads (incl. 391 MB) at once
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": alert_on_failure,
    },
    tags=["ingestion", "s3", "week1"],
)
def extract_raw_data():
    @task
    def upload(table: str, ingestion_date: str):
        return upload_one_file(table, ingestion_date)

    # One mapped task per table.
    # NOTE: manual runs have no logical_date in Airflow 3, so {{ ds }} is
    # undefined for them. run_after is always populated, and the `ds` filter
    # formats it to YYYY-MM-DD.
    upload.partial(ingestion_date="{{ dag_run.run_after | ds }}").expand(table=TABLES)


extract_raw_data()