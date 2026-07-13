from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("FintechFraudDetector") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


txn_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("location", StringType(), True)
])

# Ingesting the streaming events from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), txn_schema).alias("data")) \
    .select("data.*")

deduplicated_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["transaction_id", "timestamp"])

fraud_alerts_df = deduplicated_df \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("card_number")
    ) \
    .agg(count("transaction_id").alias("transaction_count")) \
    .filter(col("transaction_count") > 3) \
    .select(
        col("window.start").alias("alert_window_start"),
        col("window.end").alias("alert_window_end"),
        col("card_number"),
        col("transaction_count"),
        expr("current_timestamp()").alias("alert_triggered_at")
    )

# appending the alerts to Local Delta Storage And Console
query = fraud_alerts_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "output/checkpoints") \
    .start("output/delta_fraud_alerts")

query.awaitTermination()