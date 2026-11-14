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
  phone STRING COMMENT 'PII. Phone the OTP was sent to; masked for non-privileged users' MASK {G}.mask_phone,
  otp_channel STRING COMMENT 'sms or push',
  status STRING COMMENT 'sent, failed or verified',
  attempt_no INT COMMENT 'OTP attempt number within the challenge',
  _ingested_at TIMESTAMP COMMENT 'Pipeline ingestion time (bronze)',
  _source_file STRING COMMENT 'Landing file in the raw_events volume'
"""


@dp.table(
    schema=SCHEMA,
    name="silver_otp",
    comment="Silver: typed, deduplicated (on Kinesis sequenceNumber) OTP events: one row per OTP sent/failed/verified. "
            "Append-only with Change Data Feed enabled; source for failed-OTP burst features. Synthetic data.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["customer_id", "event_ts"],
)
@dp.expect_all_or_drop({
    "valid_sequence_number": "sequence_number IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_event_ts": "event_ts IS NOT NULL",
})
@dp.expect_all({
    "known_status": "status IN ('sent', 'failed', 'verified')",
    "known_channel": "otp_channel IN ('sms', 'push')",
    "positive_attempt_no": "attempt_no >= 1",
    "on_time_arrival_15m": "arrival_lag_s < 900",
})
def silver_otp():
    return (
        spark.readStream.table("bronze_otp")
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
            F.col("data.phone").alias("phone"),
            F.col("data.otp_channel").alias("otp_channel"),
            F.col("data.status").alias("status"),
            F.col("data.attempt_no").cast("int").alias("attempt_no"),
            "_ingested_at",
            "_source_file",
        )
    )
