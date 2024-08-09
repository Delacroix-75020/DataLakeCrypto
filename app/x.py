
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HDFSReadExample").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/bitcoin/data/bitcoin_data")

# Rename columns
df_renamed = df.withColumnRenamed("_c0", "timestamp") \
               .withColumnRenamed("_c1", "buy_price") \
               .withColumnRenamed("_c2", "sell_price") \
               .withColumnRenamed("_c3", "volume") \
               .withColumnRenamed("_c4", "volatility") \
               .withColumnRenamed("_c5", "change_1h") \
               .withColumnRenamed("_c6", "change_24h") \
               .withColumnRenamed("_c7", "spread")

# Cast columns to the appropriate data types
df_casted = df_renamed.withColumn("buy_price", col("buy_price").cast("double")) \
                      .withColumn("sell_price", col("sell_price").cast("double")) \
                      .withColumn("volume", col("volume").cast("double")) \
                      .withColumn("volatility", col("volatility").cast("double")) \
                      .withColumn("change_1h", col("change_1h").cast("double")) \
                      .withColumn("change_24h", col("change_24h").cast("double")) \
                      .withColumn("spread", col("spread").cast("double"))

# Show the dataframe to verify the changes
df_casted.show()

# Prepare feature columns for the model
feature_columns = ["buy_price", "volume", "volatility", "change_1h", "change_24h", "spread"]

# Assemble the feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_vectorized = assembler.transform(df_casted)

# Select features and label for the final dataset
df_final = df_vectorized.select("features", "sell_price")

# Split the data into training and test sets
train_data, test_data = df_final.randomSplit([0.8, 0.2], seed=42)

# Initialize and train the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="sell_price")
lr_model = lr.fit(train_data)

# Output the coefficients and intercept
print("Coefficients: {}".format(lr_model.coefficients))
print("Intercept: {}".format(lr_model.intercept))

# Make predictions on the test data
predictions = lr_model.transform(test_data)

# Show the predictions along with the actual values and features
predictions.select("prediction", "sell_price", "features").show()

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="sell_price", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE): {}".format(rmse))