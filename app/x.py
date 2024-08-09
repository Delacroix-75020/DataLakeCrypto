
from pyspark.sql import SparkSession

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("HDFSReadExample").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/bitcoin/data/bitcoin_data")

df_renamed = df.withColumnRenamed("_c0", "timestamp") \
               .withColumnRenamed("_c1", "buy_price") \
               .withColumnRenamed("_c2", "sell_price") \
               .withColumnRenamed("_c3", "volume") \
               .withColumnRenamed("_c4", "volatility") \
               .withColumnRenamed("_c5", "change_1h") \
               .withColumnRenamed("_c6", "change_24h") \
               .withColumnRenamed("_c7", "spread")

df_renamed.show()

feature_columns = ["buy_price", "volume", "volatility", "change_1h", "change_24h", "spread"]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_vectorized = assembler.transform(df_renamed)

df_final = df_vectorized.select("features", "sell_price")

df_final.show()

train_data, test_data = df_final.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="sell_price")

lr_model = lr.fit(train_data)

print("Coefficients: {}".format(lr_model.coefficients))
print("Intercept: {}".format(lr_model.intercept))

predictions = lr_model.transform(test_data)

predictions.select("prediction", "sell_price", "features").show()

evaluator = RegressionEvaluator(labelCol="sell_price", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE): {}".format(rmse))
