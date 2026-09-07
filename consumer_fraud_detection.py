"""Structured Streaming job that reads simulated transactions off Kafka and
flags synthetic-card velocity bursts as fraud alerts.

Submit to the local Spark cluster (see README.md "Setup & Run") with:

    docker compose exec -w /opt/spark-apps spark-master spark-submit \\
      --master spark://spark-master:7077 --deploy-mode client \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \\
      consumer_fraud_detection.py --bootstrap-servers kafka:29092

or run locally against a host-exposed Kafka for development:

    python consumer_fraud_detection.py --bootstrap-servers localhost:9092
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_set, count, expr, from_json, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)

KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2"

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "transactions"
DEFAULT_OUTPUT_PATH = "output/fraud_alerts"
DEFAULT_CHECKPOINT_PATH = "output/checkpoints"

WATERMARK_DURATION = "10 minutes"
WINDOW_DURATION = "1 minute"
SLIDE_DURATION = "30 seconds"
VELOCITY_THRESHOLD = 3

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("location", StringType(), True),
    ]
)


def create_spark_session(app_name: str, master_url: Optional[str] = None) -> SparkSession:
    """Build the SparkSession. `master_url` is only applied if explicitly
    given (via the SPARK_MASTER_URL env var) - otherwise the `--master`
    flag passed to `spark-submit` on the CLI governs, so the two don't
    fight each other.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()


def parse_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    """Read and parse the raw transaction JSON off Kafka."""
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
    )

    # from_json() turns unparseable JSON into an all-null row rather than
    # raising, so this is the actual guard against malformed/corrupt
    # records reaching the aggregation below.
    return parsed_df.filter(col("transaction_id").isNotNull())


def build_alerts_dataframe(parsed_df: DataFrame) -> DataFrame:
    """Apply the watermark, dedup, windowed velocity aggregation, and
    threshold filter that produce fraud alerts.

    `withWatermark` is a no-op annotation when this is run against a
    static/batch DataFrame (as opposed to a streaming one), so the
    dedup/windowing/threshold logic here is exercised identically in batch
    tests (see tests/test_consumer_transform.py). What batch tests *cannot*
    prove is watermark-triggered eviction of excessively late events, since
    that depends on streaming micro-batch progression - that is instead
    demonstrated live via demo/inject_late_data.py.
    """
    deduplicated_df = parsed_df.withWatermark("timestamp", WATERMARK_DURATION).dropDuplicates(
        ["transaction_id", "timestamp"]
    )

    return (
        deduplicated_df.groupBy(
            window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("card_number"),
        )
        .agg(
            count("transaction_id").alias("transaction_count"),
            collect_set("asset").alias("assets_involved"),
        )
        .filter(col("transaction_count") > VELOCITY_THRESHOLD)
        .select(
            col("window.start").alias("alert_window_start"),
            col("window.end").alias("alert_window_end"),
            col("card_number"),
            col("transaction_count"),
            col("assets_involved"),
            expr("current_timestamp()").alias("alert_triggered_at"),
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Real-time synthetic-card velocity/fraud detector.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        help="Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC),
        help="Kafka topic to subscribe to (default: %(default)s)",
    )
    parser.add_argument(
        "--output-path",
        default=os.environ.get("OUTPUT_PATH", DEFAULT_OUTPUT_PATH),
        help="Parquet output directory for alerts (default: %(default)s)",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=os.environ.get("CHECKPOINT_PATH", DEFAULT_CHECKPOINT_PATH),
        help="Structured Streaming checkpoint directory (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    spark = create_spark_session("FintechFraudDetector", master_url=os.environ.get("SPARK_MASTER_URL"))
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Starting streaming query: bootstrap_servers=%s topic=%s output_path=%s checkpoint_path=%s",
        args.bootstrap_servers,
        args.topic,
        args.output_path,
        args.checkpoint_path,
    )

    parsed_df = parse_kafka_stream(spark, args.bootstrap_servers, args.topic)
    fraud_alerts_df = build_alerts_dataframe(parsed_df)

    query = (
        fraud_alerts_df.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .start(args.output_path)
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted; stopping streaming query.")
        query.stop()


if __name__ == "__main__":
    main()
