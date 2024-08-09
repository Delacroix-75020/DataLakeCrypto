
# Bitcoin

Ce projet consiste à simuler, traiter, stocker, et analyser des flux de données sur le cours du Bitcoin en utilisant Apache Kafka, Apache Spark, et Hadoop.





## Docker-Compose

Le docker-compose.yml définit un ensemble de services pour mettre en place un environnement distribué comprenant Hadoop, Spark, Kafka, et un backend pour la simulation de données.

```python
version: "3"

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - 9870:9870
      - 9000:9000
    volumes:
      - hadoop_namenode:/hadoop/dfs/name
      - ./myhadoop:/myhadoop
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop.env

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    restart: always
    volumes:
      - hadoop_datanode:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env

  resourcemanager:
    image: bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
    container_name: resourcemanager
    restart: always
    environment:
      SERVICE_PRECONDITION: "namenode:9000 namenode:9870 datanode:9864"
    env_file:
      - ./hadoop.env

  nodemanager1:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager
    restart: always
    environment:
      SERVICE_PRECONDITION: "namenode:9000 namenode:9870 datanode:9864 resourcemanager:8088"
    env_file:
      - ./hadoop.env

  historyserver:
    image: bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8
    container_name: historyserver
    restart: always
    environment:
      SERVICE_PRECONDITION: "namenode:9000 namenode:9870 datanode:9864 resourcemanager:8088"
    volumes:
      - hadoop_historyserver:/hadoop/yarn/timeline
    env_file:
      - ./hadoop.env

  spark-master:
    image: bde2020/spark-master:3.3.0-hadoop3.3
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./app:/app
    environment:
      - INIT_DAEMON_STEP=setup_spark
      # - PATH=/spark/bin:$PATH

  spark-worker-1:
    image: bde2020/spark-worker:3.3.0-hadoop3.3
    container_name: spark-worker-1
    depends_on:
      - spark-master
    ports:
      - "8081:8081"
    environment:
      - "SPARK_MASTER=spark://spark-master:7077"

  kafka:
    image: wurstmeister/kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://:9092,INTERNAL://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092, INTERNAL://kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CREATE_TOPICS: "bitcointopic:1:1,topicdefault:1:1"
      KAFKA_LOG_RETENTION_BYTES: 1073741824
    depends_on:
      - zookeeper

  zookeeper:
    image: wurstmeister/zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"

  backend:
    build: ./producer
    container_name: backend
    restart: always
    ports:
      - "5550:5550"
    volumes:
      - ./backend:/producer
    environment:
      - FLASK_ENV=development
    depends_on:
      - kafka
      - zookeeper

volumes:
  hadoop_namenode:
  hadoop_datanode:
  hadoop_historyserver:

```
## Producer Kafka

Ce script est utilisé pour simuler la production de messages Kafka contenant des données de prix Bitcoin, qui sont ensuite envoyés à un topic Kafka nommé bitcointopic.

```python
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'bitcointopic'

def normalize(value, min_value, max_value):
    return (value - min_value) / (max_value - min_value)

def clean_and_transform(message):
    message['buy_price_normalized'] = normalize(message['buy_price'], 20000, 60000)
    message['sell_price_normalized'] = normalize(message['sell_price'], 20000, 60000)
    message['volume_normalized'] = normalize(message['volume'], 1000, 5000)
    return message

try:
    while True:
        # Simulation exceptionnelle de la volatilite du prix du Bitcoin
        message = {
            "timestamp": datetime.now().isoformat(),
            "buy_price": round(random.uniform(10000, 80000), 2),
            "sell_price": round(random.uniform(10000, 80000), 2),
            "volume": random.randint(100, 8000),
            "volatility": round(random.uniform(0.5, 5.0), 2)
        }
        message = clean_and_transform(message)
        
        producer.send(topic_name, value=message)
        print("Sent: {message}".format(message=message))
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping producer...")

producer.close()

```

## Consumer

Ce script Python utilise Apache Spark pour consommer en temps réel les données de prix du Bitcoin à partir d'un topic Kafka, les traiter, et les stocker dans HDFS.

```python
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
    .format("csv") \
    .option("path", "hdfs://namenode:9000/bitcoin/data/bitcoin_data") \
    .option("checkpointLocation", "hdfs://namenode:9000/bitcoin/data/ii") \
    .start()

query.awaitTermination()
```

## IA

Ce script utilise Apache Spark pour effectuer une analyse prédictive sur les données du Bitcoin stockées dans HDFS, en construisant un modèle de régression linéaire pour prédire le prix de vente du Bitcoin.

```python

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
```

## IA avec streamlit

Cette application a été développée pour prédire le prix du Bitcoin en temps réel. Bien que notre modèle d'intelligence artificielle complet ne soit pas encore intégré, nous avons mis en place une version simplifiée en utilisant un modèle de régression linéaire classique. Ce modèle sert à simuler le fonctionnement de l'interface en effectuant des prédictions basées sur des données financières réelles récupérées en direct.

Ce test équivalent nous permet de valider l'interface utilisateur et de démontrer le processus de prédiction en temps réel, en attendant l'intégration complète de notre solution d'IA avancée.

```python
import streamlit as st
import yfinance as yf
import pandas as pd
from sklearn.linear_model import LinearRegression
import time

# Titre de l'application
st.title("Prédiction du prix du Bitcoin en Temps Réel")

# Fonction pour charger les données
def load_data():
    data = yf.download('BTC-USD', period='1d', interval='1m')
    data['Return'] = data['Close'].pct_change()
    data = data.dropna()
    return data

# Chargement initial des données
data = load_data()

# Affichage des données récentes
st.subheader('Données récentes du Bitcoin')
st.line_chart(data['Close'])

# Entraînement du modèle initial
model = LinearRegression()
X = data[['Open', 'High', 'Low', 'Volume', 'Return']]
y = data['Close']
model.fit(X, y)

# Prédiction en temps réel
st.subheader("Prédiction en temps réel")

# Boucle pour la mise à jour en temps réel
while True:
    # Récupération des nouvelles données
    data = load_data()
    
    # Affichage des dernières données
    st.write("Dernier prix : $", data['Close'].iloc[-1])

    # Prédiction du prix à partir des dernières données
    latest_data = data.iloc[-1][['Open', 'High', 'Low', 'Volume', 'Return']].values.reshape(1, -5)
    prediction = model.predict(latest_data)
    
    # Affichage de la prédiction
    st.write(f"Le prix prédit du Bitcoin est : ${prediction[0]:.2f}")
    
    # Mise à jour toutes les 60 secondes
    time.sleep(5)
    st.experimental_rerun()

```
