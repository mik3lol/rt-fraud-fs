from pyspark import pipelines as dp
from pyspark.sql import functions as F

G = spark.conf.get("governance_schema")  # catalog.schema holding the UC mask functions (created by 03_governance)

SCHEMA = f"""
  sequence_number STRING COMMENT 'Kinesis sequenceNumber; dedup key',
  partition_key STRING COMMENT 'Kinesis partitionKey (= customer_id)',
  event_id STRING COMMENT 'Event UUID',
  customer_id STRING COMMENT 'Synthetic customer key',
  event_ts TIMESTAMP COMMENT 'Event time (UTC) reported by the app',
  arrival_ts TIMESTAMP COMMENT 'Kinesis approximateArrivalTimestamp (UTC)',
  arrival_lag_s DOUBLE COMMENT 'Seconds between event_ts and arrival_ts',
  session_id STRING COMMENT 'App session id',
  device_id STRING COMMENT 'PII. Device identifier; masked for non-privileged users' MASK {G}.mask_device_id,
  screen_name STRING COMMENT 'App screen, e.g. send_money, security_settings',
  screen_seq INT COMMENT 'Order of the screen within the session',
  dwell_ms INT COMMENT 'Time spent on the screen in milliseconds',
  _ingested_at TIMESTAMP COMMENT 'Pipeline ingestion time (bronze)',
  _source_file STRING COMMENT 'Landing file in the raw_events volume'
"""


@dp.table(
    schema=SCHEMA,
    name="silver_screen_view",
    comment="Silver: typed, deduplicated (on Kinesis sequenceNumber) app screen views with dwell time. "
            "Append-only with Change Data Feed enabled; source for screen-hop and Last-N screen features. Synthetic data.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["customer_id", "event_ts"],
)
@dp.expect_all_or_drop({
    "valid_sequence_number": "sequence_number IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_event_ts": "event_ts IS NOT NULL",
})
@dp.expect_all({
    "non_negative_dwell": "dwell_ms >= 0",
    "has_screen_name": "screen_name IS NOT NULL",
    "on_time_arrival_15m": "arrival_lag_s < 900",
})
def silver_screen_view():
    return (
        spark.readStream.table("bronze_screen_view")
        .withWatermark("_ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["sequenceNumber"])
        .withColumn("event_ts", F.try_to_timestamp(F.col("data.event_ts")))
        .withColumn("arrival_ts", F.timestamp_seconds(F.col("approximateArrivalTimestamp")))
        .select(
            F.col("sequenceNumber").alias("sequence_number"),
            F.col("partitionKey").alias("partition_key"),
            F.col("data.event_id").alias("event_id"),
            F.col("data.customer_id").alias("customer_id"),
            "event_ts",
            "arrival_ts",
            F.round(F.col("approximateArrivalTimestamp") - F.unix_micros("event_ts") / 1e6, 3).alias("arrival_lag_s"),
            F.col("data.session_id").alias("session_id"),
            F.col("data.device_id").alias("device_id"),
            F.col("data.screen_name").alias("screen_name"),
            F.col("data.screen_seq").cast("int").alias("screen_seq"),
            F.col("data.dwell_ms").cast("int").alias("dwell_ms"),
            "_ingested_at",
            "_source_file",
        )
    )
