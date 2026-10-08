"""
clean_table.py -- clean one raw table and land it in the processed zone.

    spark-submit clean_table.py --table patients --ingestion-date 2026-10-02
    spark-submit clean_table.py --ingestion-date 2026-10-02      # every table

STEPS
-----
  1. read the raw CSV with the explicit schema from schemas.py
  2. drop duplicate rows on the table's primary key
  3. drop rows whose primary key is incomplete (any key column null)
  4. stamp _ingested_date so the output is partitioned per run
  5. report row counts and remaining nulls (the first data-quality gate)
  6. write Parquet to processed/<table>/_ingested_date=<date>/

Omit --table to process every table in one Spark session. That is the right
call locally (one JVM instead of eleven) but NOT under Airflow -- there you want
one task per table so a single failure retries on its own.

WHY DUPLICATES AND NULL KEYS ARE COUNTED SEPARATELY
---------------------------------------------------
They are different problems. Duplicates mean the source repeated a row. Null
keys mean the primary key is WRONG -- it includes a column that is legitimately
empty, so valid rows get discarded.

Lumping them into one "dropped" number hides the second case completely. That
is not hypothetical: payer_transitions originally keyed on
(PATIENT, MEMBERID, START_DATE, PAYER), and MEMBERID is null for all 10,521
government-payer rows. It looked like ordinary deduplication while quietly
throwing away 28% of the table.
"""
import argparse
import sys
from functools import reduce

from pyspark.sql import functions as F

from common import build_spark, null_report, raw_path, write_output
from schemas import TABLES


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Clean one raw table.")
    p.add_argument("--table", choices=sorted(TABLES), help="omit to clean every table")
    p.add_argument("--ingestion-date", required=True)
    p.add_argument("--bucket", default=None)
    return p.parse_args(argv)


def complete_key(spec):
    """Condition that every primary-key column is non-null.

    A row with a partially-null key cannot be deduplicated reliably, so it is
    dropped. reduce() over a single column just returns that column.
    """
    return reduce(lambda a, b: a & b, (F.col(c).isNotNull() for c in spec.primary_key))


def clean(spark, table, ingestion_date, bucket=None):
    """Return (cleaned_df, counts).

    counts separates the two drop reasons. That costs an extra pass over the
    data, which is worth it: it is the difference between "73 rows went"
    (fine) and "10,521 rows went" (a broken primary key).
    """
    spec = TABLES[table]

    df = (
        spark.read.option("header", True)
        .schema(spec.schema)
        .csv(raw_path(table, ingestion_date, bucket))
    )
    raw_rows = df.count()

    deduped = df.dropDuplicates(list(spec.primary_key))
    deduped_rows = deduped.count()

    df = deduped.filter(complete_key(spec))
    df = df.withColumn("_ingested_date", F.lit(ingestion_date).cast("date"))
    clean_rows = df.count()

    counts = {
        "raw": raw_rows,
        "duplicates": raw_rows - deduped_rows,
        "null_keys": deduped_rows - clean_rows,
        "clean": clean_rows,
    }
    return df, counts


def report(df, table, counts):
    spec = TABLES[table]
    print(f"table        : {table}")
    print(f"primary key  : {', '.join(spec.primary_key)}")
    print(
        f"rows         : {counts['raw']} raw -> {counts['clean']} clean "
        f"(duplicates {counts['duplicates']}, null keys {counts['null_keys']})"
    )

    nulls = null_report(df).first()
    remaining = {c: (nulls[c] or 0) for c in df.columns if (nulls[c] or 0) > 0}
    if remaining:
        print("columns still holding nulls (all expected -- see schemas.py):")
        for col, n in sorted(remaining.items(), key=lambda kv: -kv[1]):
            print(f"  {col:<26} {n}")
    else:
        print("nulls        : none in any column")


def main(argv=None):
    args = parse_args(argv)
    tables = [args.table] if args.table else sorted(TABLES)

    spark = build_spark("clean-raw")
    failed = []
    try:
        for table in tables:
            try:
                df, counts = clean(spark, table, args.ingestion_date, args.bucket)
                report(df, table, counts)

                target = write_output(
                    df,
                    table,
                    partition_by=["_ingested_date"],
                    bucket=args.bucket,
                )
                print(f"wrote        : {target}\n")
            except Exception as exc:
                # Keep going: one bad table should not block the other ten.
                print(
                    f"FAILED       : {table} -> {type(exc).__name__}: {exc}\n",
                    file=sys.stderr,
                )
                failed.append(table)
    finally:
        spark.stop()

    if failed:
        print(
            f"RESULT: {len(tables) - len(failed)}/{len(tables)} written; "
            f"FAILED: {', '.join(failed)}"
        )
        return 1

    print(f"RESULT: {len(tables)} table(s) written to processed/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
