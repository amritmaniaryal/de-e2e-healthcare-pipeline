import argparse
from common import build_spark, write_output, null_report
from schemas import TABLES

p = argparse.ArgumentParser()
p.add_argument("--table", required=True)
p.add_argument("--ingestion-date", required=True)
args = p.parse_args()

spec = TABLES[args.table]
spark = build_spark()

df = (spark.read
      .option("header", True)
      .schema(spec.schema)
      .csv(f"s3a://{BUCKET}/raw/{args.table}/ingestion_date={args.ingestion_date}/{args.table}.csv"))

df = df.dropDuplicates([spec.primary_key]).filter(f"{spec.primary_key} IS NOT NULL") \
       .withColumn("_ingested_date", F.lit(args.ingestion_date).cast("date"))