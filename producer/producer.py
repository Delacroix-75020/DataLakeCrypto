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

try:
    while True:
        message = {
            "timestamp": datetime.now().isoformat(),
            "buy_price": round(random.uniform(20000, 60000), 2),
            "sell_price": round(random.uniform(20000, 60000), 2),
            "volume": random.randint(1000, 5000),
            "volatility": round(random.uniform(0.5, 5.0), 2)
        }
        
        producer.send(topic_name, value=message)
        print("Sent: {message}".format(message=message))
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping producer...")

producer.close()
