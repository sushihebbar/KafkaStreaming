from pyspark.sql.functions import col, split, count
from pyspark.sql.functions import window, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import SparkSession
import random
topic = ""
print("WELCOME TO SPARK BATCH CONSUMER")
print("Enter the topic you want to choose : \n 1. top_captions \n 2. top_hashtags \n 3. captions \n 4. hashtags \n 5. random topic\n")
#choice = random.randint(1, 4)
choice = 2
print("The chosen topic is : ", choice)
if choice == 1:
    topic = "top_captions"
elif choice == 2:
    topic = "top_hashtags"
elif choice == 3:
    topic = "captions"
elif choice == 4:
    topic = "hashtags"

spark = SparkSession.builder.appName("InstaCount").getOrCreate()

# Define the schema for the Kafka topic
schema = StructType([
    StructField("caption_id", LongType(), True),
    StructField("caption", StringType(), True),
    StructField("date_time", StringType(), True),
    StructField("language", StringType(), True)
])

# Read the Kafka topic as a streaming DataFrame
df_captions = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "captions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data in the value column
df_captions_parsed = df_captions.select(from_json(col("value").cast("string"), schema).alias("insta_db"), col("timestamp"))

# Extract the fields from the parsed JSON data
df_captions_processed = df_captions_parsed.select("insta_db.*", "timestamp") \
    .withColumn("date_time", to_timestamp("date_time", "yyyy-MM-dd HH:mm:ss"))

# Define the window duration for the batch processing
window_duration = "5 minutes"

# Group the captions by language and timestamp window and count the captions
df_caption_counts = df_captions_processed \
    .withWatermark("timestamp", window_duration) \
    .groupBy(window("timestamp", window_duration), "language") \
    .agg(count("*").alias("caption_count"))

# Print the results to the console
query_captions = df_caption_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for the query to terminate
query_captions.awaitTermination()
