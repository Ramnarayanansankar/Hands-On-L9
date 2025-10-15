# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
data = spark.readStream.format("socket") \
.option("host", "localhost").option("port", 9999) \
.load()

# Parse JSON data into columns using the defined schema
parsed_data = data.select(from_json(col("value").cast("string"), schema) \
.alias("data")).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
data_with_timestamp = parsed_data.withColumn("event_time", col("timestamp") \
.cast(TimestampType()))
data_with_watermark = data_with_timestamp.withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
data_aggregations = data_with_watermark.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(_sum("fare_amount").alias("total_fare"))

# Extract window start and end times as separate columns
final_data = data_aggregations.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch_to_csv(df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included
    df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"outputs/task3_outputs/batch_{batch_id}")

# Use foreachBatch to apply the function to each micro-batch
query = final_data.writeStream \
.foreachBatch(write_batch_to_csv) \
.outputMode("append") \
.option("checkpointLocation", "checkpoints/task3_checkpoint") \
.start()

query.awaitTermination()