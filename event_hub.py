# Databricks notebook source
# MAGIC %pip install azure-eventhub

# COMMAND ----------

from azure.eventhub import EventHubProducerClient, EventData
import time
from datetime import datetime

# COMMAND ----------

connection_str = "Endpoint=sb://pocstreaming.servicebus.windows.net/;SharedAccessKeyName=Mykey;SharedAccessKey=1QTRVLVNRheysGeQLhg8K1m/Fna6d/0h5+AEhKbqAfk=;EntityPath=hdi_streaming"
eventhub_name = "hdi_streaming"
delay_between_chunks = 0.5  # seconds delay after every 1000 messages
messages_per_batch = 500_000
throttle_chunk_size = 1000

# COMMAND ----------

# Initialize Event Hub Producer
producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str,
    eventhub_name=eventhub_name
)

# COMMAND ----------

# Retry logic if throttled
def retry_send(batch, max_retries=3):
    retries = 0
    while retries <= max_retries:
        try:
            producer.send_batch(batch)
            break
        except Exception as e:
            if "throttled" in str(e).lower():
                wait_time = 5 + retries * 2  
                time.sleep(wait_time)
                retries += 1
            else:
                raise e

# COMMAND ----------



# Function to send 5 lakh messages with retry and delay
def send_bulk_messages(batch_num):
    print(f"\n Starting Batch {batch_num} — Generating {messages_per_batch} messages...")
    start_time = time.time()
    start_readable = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    message_count = 0
    total_sent_batches = 0
    batch = producer.create_batch()

    for i in range(messages_per_batch):
        msg = f"Batch {batch_num} - Message {i+1} at {start_readable}"

        try:
            batch.add(EventData(msg))
            message_count += 1
        except ValueError:
            # Batch full — send it with retry
            retry_send(batch)
            total_sent_batches += 1
            batch = producer.create_batch()
            batch.add(EventData(msg))
            message_count += 1

        # Throttle every 1000 messages
        if i % throttle_chunk_size == 0 and i != 0:
            time.sleep(delay_between_chunks)

    # Send last remaining batch
    if len(batch) > 0:
        retry_send(batch)
        total_sent_batches += 1

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    # Summary
    print(f"""
  Batch {batch_num} Summary:
 Total messages sent     : {message_count}
 Total batches pushed    : {total_sent_batches}
 Start time              : {start_readable}
 Time taken (seconds)    : {duration}
 Waiting 1 minute before next batch...
""")




# COMMAND ----------

# Infinite loop to generate every 1 minute
batch_num = 1
while True:
    send_bulk_messages(batch_num)
    batch_num += 1
    time.sleep(60)  # Wait 1 minute before next batch