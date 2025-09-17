import time
import json
from kafka import KafkaConsumer

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    'bank_transactions',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest', # Start from the beginning of the topic
    group_id='bank-group'
)

print("Starting to consume and process messages...")
batch = []
start_time = time.time()

for message in consumer:
    transaction = message.value
    batch.append(transaction)
    
    current_time = time.time()
    if current_time - start_time >= 15:
        print("--- 15-SECOND BATCH ANALYSIS ---")
        total_transactions = len(batch)
        total_amount = sum(t['amount'] for t in batch)
        print(f"Processed {total_transactions} transactions.")
        print(f"Total amount for this batch: {total_amount:.2f}")
        
        # Reset the batch and timer for the next window
        batch = []
        start_time = time.time()
