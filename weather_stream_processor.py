from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

weather_schema = StructType() \
    .add("timestamp", StringType()) \
    .add("city", StringType()) \
    .add("temp_c", DoubleType()) \
    .add("humidity", IntegerType()) \
    .add("weather", StringType()) \
    .add("description", StringType())

spark = SparkSession.builder.appName("WeatherStreamProcessor").getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_raw") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), weather_schema)) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

output_path = "s3a://your-bucket-name/weather/processed/"
checkpoint_path = "s3a://your-bucket-name/weather/checkpoints/"

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("city") \
    .start()

query.awaitTermination()