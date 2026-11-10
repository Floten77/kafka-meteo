
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("WeatherStreaming")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_alert", StringType()),
    StructField("heat_alert", StringType())
])


df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_transformed")
    .option("startingOffsets", "earliest")
    .load()
)


df_weather = (
    df_kafka
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
    .filter(F.col("event_time").isNotNull())
)


df_watermark = df_weather.withWatermark("event_time", "1 minute")


df_aggregates = (
    df_watermark
    .groupBy(
        F.window("event_time", "1 minute", "30 seconds"),
        F.col("country"),
        F.col("city")
    )
    .agg(
        F.sum(F.when(F.col("wind_alert") == "level_1", 1).otherwise(0)).alias("wind_level_1"),
        F.sum(F.when(F.col("wind_alert") == "level_2", 1).otherwise(0)).alias("wind_level_2"),
        F.sum(F.when(F.col("heat_alert") == "level_1", 1).otherwise(0)).alias("heat_level_1"),
        F.sum(F.when(F.col("heat_alert") == "level_2", 1).otherwise(0)).alias("heat_level_2"),
        F.avg("temperature").alias("avg_temperature"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature")
    )
)


df_aggregates = df_aggregates.withColumn(
    "total_alerts",
    F.col("wind_level_1") +
    F.col("wind_level_2") +
    F.col("heat_level_1") +
    F.col("heat_level_2")
)


query = (
    df_aggregates.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .option("numRows", 100)
    .option(
        "checkpointLocation",
        "/home/jovyan/work/checkpoints/weather_aggregates"
    )
    .start()
)

print("Streaming des agrégats démarré")

query.awaitTermination()
