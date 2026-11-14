from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW = spark.conf.get("raw_events_path")


@dp.table(
    name="bronze_otp",
    comment="Bronze: raw Kinesis-shaped OTP records (sent/failed/verified) ingested append-only by Auto Loader "
            "from the raw_events/otp volume folder. Synthetic data.",
    table_properties={"quality": "bronze"},
    cluster_by_auto=True,
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
@dp.expect("has_sequence_number", "sequenceNumber IS NOT NULL")
def bronze_otp():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "partitionKey STRING, sequenceNumber STRING, approximateArrivalTimestamp DOUBLE")
        .load(f"{RAW}/otp")
        .select("*", F.current_timestamp().alias("_ingested_at"), F.col("_metadata.file_path").alias("_source_file"))
    )
