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
  ip STRING COMMENT 'PII. Client IPv4; masked for non-privileged users' MASK {G}.mask_ip,
  geo_country STRING COMMENT 'ISO-2 country from IP geolocation',
  auth_method STRING COMMENT 'password or biometric',
  success BOOLEAN COMMENT 'True if the login attempt succeeded',
  failure_reason STRING COMMENT 'Failure reason when success = false',
  app_version STRING COMMENT 'Mobile app version',
  _ingested_at TIMESTAMP COMMENT 'Pipeline ingestion time (bronze)',
  _source_file STRING COMMENT 'Landing file in the raw_events volume'
"""


@dp.table(
    schema=SCHEMA,
    name="silver_login",
    comment="Silver: typed, deduplicated (on Kinesis sequenceNumber) login events, one row per login attempt. "
            "Append-only with Change Data Feed enabled; source for per-customer streaming features. Synthetic data.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["customer_id", "event_ts"],
)
@dp.expect_all_or_drop({
    "valid_sequence_number": "sequence_number IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_event_ts": "event_ts IS NOT NULL",
})
@dp.expect_all({
    "known_auth_method": "auth_method IN ('password', 'biometric')",
    "has_device_id": "device_id IS NOT NULL",
    "on_time_arrival_15m": "arrival_lag_s < 900",
})
def silver_login():
    return (
        spark.readStream.table("bronze_login")
        # Kinesis consumers are at-least-once: drop replays of the same sequenceNumber seen within 1h of ingestion.
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
            F.col("data.ip").alias("ip"),
            F.col("data.geo_country").alias("geo_country"),
            F.col("data.auth_method").alias("auth_method"),
            F.col("data.success").cast("boolean").alias("success"),
            F.col("data.failure_reason").alias("failure_reason"),
            F.col("data.app_version").alias("app_version"),
            "_ingested_at",
            "_source_file",
        )
    )
