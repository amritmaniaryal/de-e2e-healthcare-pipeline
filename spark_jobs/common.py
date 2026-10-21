"""
Shared helpers for the Spark jobs.

Everything here is identical across tables. Per-table differences belong in
schemas.py; per-table entry points (clean_table.py, validate_table.py) stay
thin and just wire things together.
"""
import os

from pyspark.sql import SparkSession, functions as F

# docker-compose injects this from ../.env. Kept as a module constant so jobs
# can reference it directly; the path helpers raise clearly if it is unset.
BUCKET = os.environ.get("S3_BUCKET")

RAW_ZONE = "raw"
PROCESSED_ZONE = "processed"


def _require_bucket(bucket=None):
    resolved = bucket or BUCKET
    if not resolved:
        raise RuntimeError(
            "S3_BUCKET is not set. It comes from ../.env via docker-compose "
            "env_file. Pass bucket=... explicitly to override."
        )
    return resolved


def build_spark(app_name: str) -> SparkSession:
    """Spark session with s3a pointed at the container's env credentials.

    Both s3a settings are required, and identical for every job:
      * hadoop-aws does NOT read AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY by
        itself -- you must name the provider class explicitly. It lives in the
        AWS SDK bundle, not in hadoop-aws (whose only providers are Simple,
        Temporary, Anonymous and IAMInstance).
      * the endpoint region must be explicit, or s3a guesses wrong.
    """
    return (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app_name)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
        )
        # Spark writes a zero-byte _SUCCESS marker into every output directory.
        # It is bookkeeping, not data, and every S3 reader (Snowflake COPY INTO,
        # Athena, DuckDB, pandas) has to be told to skip it. Turn it off here
        # instead of working around it at each reader.
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs",
            "false",
        )
        .getOrCreate()
    )


def raw_path(table: str, ingestion_date: str, bucket: str | None = None) -> str:
    """The untouched CSV landed by the extract_raw_data DAG."""
    b = _require_bucket(bucket)
    return f"s3a://{b}/{RAW_ZONE}/{table}/ingestion_date={ingestion_date}/{table}.csv"


def raw_prefix(table: str, bucket: str | None = None) -> str:
    """All ingestion dates for one table -- for historical / backfill reads."""
    b = _require_bucket(bucket)
    return f"s3a://{b}/{RAW_ZONE}/{table}/"


def processed_path(table: str, bucket: str | None = None) -> str:
    """The cleaned Parquet zone."""
    b = _require_bucket(bucket)
    return f"s3a://{b}/{PROCESSED_ZONE}/{table}/"


def read_raw(spark, table, ingestion_date, schema=None, bucket=None):
    """Read one raw CSV.

    With schema=None every column comes back as string. validate_table.py
    depends on exactly that difference, so keep the default that way.
    """
    reader = spark.read.option("header", True)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.csv(raw_path(table, ingestion_date, bucket))


def null_report(df):
    """One-row DataFrame: the null count for every column.

    Spark has no df.info(); this is the closest equivalent to its
    'non-null count' column. Call .first() to pull it to the driver.
    """
    return df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    )


def write_output(df, table, partition_by=None, bucket=None, mode="overwrite"):
    """Write cleaned data to processed/ as Parquet.

    Pass partition_by=["_ingested_date"] to match the pipeline convention of
    one directory per ingestion run, which makes backfills replayable.
    """
    target = processed_path(table, bucket)
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(target)
    return target