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
