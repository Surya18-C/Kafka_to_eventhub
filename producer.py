
from confluent_kafka import Producer
import json
import time

conf = {
    'bootstrap.servers': 'pocstreaming.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://pocstreaming.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4DY45iz4hcBkrPSQnyAcXwk/E16u01sK6+AEhNvsVyI='
}


producer = Producer(conf)

for i in range(1, 101):
    msg = {"order_id": i, "product": f"Item-{i}", "price": i * 10, "timestamp": "2025-06-27 10:00:00"}
    producer.produce("hdi_streaming", value=json.dumps(msg))
    print(f"Sent: {msg}")
    time.sleep(0.1)

producer.flush()