
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HDFSReadExample").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/bitcoin/data/bitcoin_data")
df.show()
