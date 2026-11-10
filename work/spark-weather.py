
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("WeatherStreaming")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("msg", StructType([
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("weather", StructType([
            StructField("current", StructType([
                StructField("time", StringType()),
                StructField("temperature_2m", DoubleType()),
                StructField("wind_speed_10m", DoubleType())
            ]))
        ]))
    ]))
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_stream")
    .option("startingOffsets", "earliest")
    .load()
)


df_json = df_kafka.select(
    F.col("timestamp").alias("event_time"),
    F.from_json(F.col("value").cast("string"), schema).alias("data")
)

df_weather = (
    df_json
    .select(
        F.col("event_time"),
        F.col("data.msg.city").alias("city"),
        F.col("data.msg.country").alias("country"),
        F.col("data.msg.weather.current.temperature_2m").alias("temperature"),
        F.col("data.msg.weather.current.wind_speed_10m").alias("wind_speed")
    )
)

#ALERT
df_weather_transformed = (
    df_weather
    .withColumn(
        "wind_alert",
        F.when(F.col("wind_speed") < 10, "level_0")
         .when(F.col("wind_speed") <= 20, "level_1")
         .otherwise("level_2")
    )
    .withColumn(
        "heat_alert",
        F.when(F.col("temperature") < 25, "level_0")
         .when(F.col("temperature") <= 35, "level_1")
         .otherwise("level_2")
    )
)
df_output = df_weather_transformed.select(
    F.col("city").cast("string").alias("key"),
    F.to_json(
        F.struct(
            "event_time",
            "city",
            "country",
            "temperature",
            "wind_speed",
            "wind_alert",
            "heat_alert"
        )
    ).alias("value")
)

query = (
    df_output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "weather_transformed")
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/weather_transformed")
    .outputMode("append")
    .start()
)
print("Streaming démarré : weather_stream → weather_transformed")

query.awaitTermination()
