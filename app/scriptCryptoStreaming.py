
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Creer une session Spark
spark = SparkSession.builder \
    .appName("BitcoinPriceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Definir le schema des donnees JSON que nous recevons
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("buy_price", DoubleType(), True),
    StructField("sell_price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("volatility", DoubleType(), True)
])

# Lire le flux de donnees a partir de Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "bitcointopic") \
  .load()

# Convertir la valeur du message Kafka de binaire a JSON
df = df.selectExpr("CAST(value AS STRING) as json_data")

# Appliquer le schema JSON
df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# Afficher les donnees sur la console (optionnel)
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Attendre la fin du flux
query.awaitTermination()