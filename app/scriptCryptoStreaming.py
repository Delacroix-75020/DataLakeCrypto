
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("BitcoinPriceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("buy_price", DoubleType(), True),
    StructField("sell_price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("volatility", DoubleType(), True),
    StructField("buy_price_normalized", DoubleType(), True),
    StructField("sell_price_normalized", DoubleType(), True),
    StructField("volume_normalized", DoubleType(), True)
])

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "bitcointopic") \
  .load()

df = df.selectExpr("CAST(value AS STRING) as json_data")
df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

query = df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/bitcoin/data/bitcoin_data") \
    .option("checkpointLocation", "hdfs://namenode:9000/bitcoin/data/ii") \
    .start()

query.awaitTermination()