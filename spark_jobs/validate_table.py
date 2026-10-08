"""
validate_table.py -- data-quality gate: typed read vs untyped read.

    spark-submit validate_table.py --table patients --ingestion-date 2026-10-02
    spark-submit validate_table.py --ingestion-date 2026-10-02      # all tables

HOW IT WORKS
------------
Read the SAME raw CSV from S3 twice, with the same bytes, changing one thing --
whether the schema is applied:

  1. NO schema  -> every column is string. In this read the ONLY thing that can
     produce a null is an EMPTY FIELD, because no cast can happen. So its null
     count per column IS the true blank count.

  2. declared schema from schemas.py -> a null now means EITHER an empty field
     OR a value that FAILED TO CAST.

So for every column:  typed_nulls - blank_fields = number of silently lost
values. Any column where that is > 0 means the schema is wrong for the data
(or the data contains values the declared type cannot hold).

Why this matters: a failed cast does NOT error and does NOT drop the row. Spark
substitutes null. Row counts therefore look perfect while the data is quietly
corrupt -- a 17-digit SNOMED code declared `int` becomes null with no warning.

Exit code is 1 if any column fails, so an Airflow task fails with it.

NOTE ON CSV OPTIONS: Spark's csv reader turns an empty field into null no matter
what `nullValue` / `emptyValue` are set to (verified on Spark 3.5.3 against a
10-row fixture). That is exactly why the untyped read above is the reliable way
to measure blank fields -- it needs no options at all.
"""
import argparse
import os
import sys

from common import build_spark, null_report, raw_path
from schemas import TABLES


def validate(spark, table: str, ingestion_date: str, bucket: str, schema_override=None):
    """Return (columns, rows, blank_counts, failures).

    failures is a list of (column, blank_fields, typed_nulls) for columns where
    the typed read produced more nulls than the untyped read.
    """
    if table not in TABLES:
        raise KeyError(f"unknown table {table!r}; known: {', '.join(sorted(TABLES))}")

    spec = TABLES[table]
    path = raw_path(table, ingestion_date, bucket)

    untyped = spark.read.option("header", True).csv(path)
    typed = (
        spark.read.option("header", True)
        .schema(schema_override or spec.schema)
        .csv(path)
    )

    # null_report() returns a one-row DataFrame (lazy); .first() materialises it
    # into a Row we can index by column name.
    untyped_nulls = null_report(untyped).first()
    typed_nulls = null_report(typed).first()

    columns = untyped.columns
    blanks = {c: (untyped_nulls[c] or 0) for c in columns}
    failures = [
        (c, blanks[c], typed_nulls[c] or 0)
        for c in columns
        if (typed_nulls[c] or 0) > blanks[c]
    ]

    # Both reads must see the same number of rows. They always should -- a
    # schema cannot add or remove rows -- so a mismatch means something else is
    # wrong, and is reported separately from cast failures.
    untyped_rows = untyped.count()
    typed_rows = typed.count()

    return columns, untyped_rows, typed_rows, blanks, failures


def main() -> int:
    p = argparse.ArgumentParser(description="Data-quality gate for raw tables.")
    p.add_argument("--table", help="table name; omit to validate every table")
    p.add_argument("--ingestion-date", required=True)
    p.add_argument("--bucket", default=os.environ.get("S3_BUCKET"))
    args = p.parse_args()

    if not args.bucket:
        print("ERROR: S3_BUCKET is not set and --bucket was not passed", file=sys.stderr)
        return 2

    tables = [args.table] if args.table else sorted(TABLES)
    spark = build_spark("validate-raw")
    failed = []

    try:
        for table in tables:
            columns, rows, typed_rows, blanks, failures = validate(
                spark, table, args.ingestion_date, args.bucket
            )
            blank_cols = sum(1 for c in columns if blanks[c] > 0)
            row_mismatch = rows != typed_rows
            bad = bool(failures) or row_mismatch

            print(
                f"{'FAIL' if bad else 'PASS':<10} {table:<22} rows={rows:>7} "
                f"cols={len(columns):>3} columns_with_blanks={blank_cols:>3}"
            )
            if row_mismatch:
                print(f"           ROW COUNT MISMATCH: untyped={rows} typed={typed_rows}")
            for col, expected, actual in failures:
                print(
                    f"           {col:<26} blank_fields={expected:<9} "
                    f"typed_nulls={actual:<9} lost={actual - expected}"
                )
            if bad:
                failed.append(table)
    finally:
        spark.stop()

    print()
    if failed:
        print(f"RESULT: FAIL -- {len(failed)} table(s): {', '.join(failed)}")
        return 1

    print(f"RESULT: PASS -- {len(tables)} table(s), no values lost to failed casts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
