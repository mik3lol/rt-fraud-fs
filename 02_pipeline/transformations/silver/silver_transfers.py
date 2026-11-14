from pyspark import pipelines as dp
from pyspark.sql import functions as F

CORRIDORS = "('US-MX', 'US-PH', 'US-IN', 'US-GT', 'US-NG')"
G = spark.conf.get("governance_schema")  # catalog.schema holding the UC mask functions (created by 03_governance)

SCHEMA = f"""
  sequence_number STRING COMMENT 'Kinesis sequenceNumber; dedup key',
  partition_key STRING COMMENT 'Kinesis partitionKey (= customer_id)',
  transfer_id STRING COMMENT 'Transfer UUID',
  customer_id STRING COMMENT 'Synthetic customer key (sender)',
  ts TIMESTAMP COMMENT 'Transfer creation time (UTC)',
  arrival_ts TIMESTAMP COMMENT 'Kinesis approximateArrivalTimestamp (UTC)',
  arrival_lag_s DOUBLE COMMENT 'Seconds between ts and arrival_ts',
  amount_usd DECIMAL(12,2) COMMENT 'Send amount in USD',
  currency STRING COMMENT 'Send currency (USD)',
  corridor STRING COMMENT 'Send corridor, e.g. US-MX, US-PH, US-IN',
  payout_method STRING COMMENT 'bank_deposit, cash_pickup, mobile_wallet or home_delivery',
  recipient_id STRING COMMENT 'Recipient key',
  device_id STRING COMMENT 'PII. Device used to create the transfer; masked for non-privileged users' MASK {G}.mask_device_id,
  ip STRING COMMENT 'PII. Client IPv4; masked for non-privileged users' MASK {G}.mask_ip,
  is_fraud INT COMMENT 'Synthetic ground-truth label (1 = fraud). Training only, never a model input',
  _ingested_at TIMESTAMP COMMENT 'Pipeline ingestion time (bronze)',
  _source_file STRING COMMENT 'Landing file in the raw_events volume'
"""


@dp.table(
    schema=SCHEMA,
    name="silver_transfers",
    comment="Silver: typed, deduplicated (on Kinesis sequenceNumber) remittance transfers, one row per transfer_id. "
            "is_fraud is the synthetic ground-truth label (training only, never a model input). "
            "Append-only with Change Data Feed enabled. Synthetic data.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["customer_id", "ts"],
)
@dp.expect_all_or_drop({
    "valid_sequence_number": "sequence_number IS NOT NULL",
    "valid_transfer_id": "transfer_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_ts": "ts IS NOT NULL",
    "positive_amount": "amount_usd > 0",
})
@dp.expect_all({
    "known_corridor": f"corridor IN {CORRIDORS}",
    "known_payout_method": "payout_method IN ('bank_deposit', 'cash_pickup', 'mobile_wallet', 'home_delivery')",
    "amount_under_10k": "amount_usd < 10000",
    "label_is_binary": "is_fraud IN (0, 1)",
    "on_time_arrival_15m": "arrival_lag_s < 900",
})
def silver_transfers():
    return (
        spark.readStream.table("bronze_transfers")
        .withWatermark("_ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["sequenceNumber"])
        .withColumn("ts", F.try_to_timestamp(F.col("data.ts")))
        .withColumn("arrival_ts", F.timestamp_seconds(F.col("approximateArrivalTimestamp")))
        .select(
            F.col("sequenceNumber").alias("sequence_number"),
            F.col("partitionKey").alias("partition_key"),
            F.col("data.transfer_id").alias("transfer_id"),
            F.col("data.customer_id").alias("customer_id"),
            "ts",
            "arrival_ts",
            F.round(F.col("approximateArrivalTimestamp") - F.unix_micros("ts") / 1e6, 3).alias("arrival_lag_s"),
            F.col("data.amount_usd").cast("decimal(12,2)").alias("amount_usd"),
            F.col("data.currency").alias("currency"),
            F.col("data.corridor").alias("corridor"),
            F.col("data.payout_method").alias("payout_method"),
            F.col("data.recipient_id").alias("recipient_id"),
            F.col("data.device_id").alias("device_id"),
            F.col("data.ip").alias("ip"),
            F.col("data.is_fraud").cast("int").alias("is_fraud"),
            "_ingested_at",
            "_source_file",
        )
    )
