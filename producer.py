import time
import json
from kafka import KafkaProducer

# Create a Kafka producer instance
# The value_serializer ensures the message is a JSON string
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'bank_transactions'

print("Starting to produce messages...")
transaction_id = 0

while True:
    transaction = {
        'id': transaction_id,
        'account': f'ACC{transaction_id % 10}',
        'amount': round(50 + (transaction_id * 1.5), 2),
        'timestamp': time.time()
    }
    
    producer.send(topic_name, value=transaction)
    print(f"Sent transaction: {transaction}")
    transaction_id += 1
    time.sleep(5) # Send a message every 5 seconds

producer.flush()
producer.close()
