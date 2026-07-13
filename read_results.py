from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadFraudAlerts") \
    .getOrCreate()

# Read the generated parquet files in the delta fraud alerts directory
df = spark.read.parquet("output/delta_fraud_alerts")

print("Detected fraud bursts recorded on disk \n")
df.show(truncate=False)