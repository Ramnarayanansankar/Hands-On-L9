from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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

# Print parsed data to the CSV files
query_task1 = parsed_data.writeStream.format("csv").outputMode("append") \
.option("header", "true").option("path", "outputs/task1_outputs") \
.option("checkpointLocation", "checkpoints/task1_checkpoint").start()

query_task1.awaitTermination()