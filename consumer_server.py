import asyncio
from kafka import KafkaConsumer
import json

async def consume(topic_name):
    group_id = "0"
    bootstrap_servers = ["localhost:9092"]
    
    consumer = KafkaConsumer(group_id = group_id,
                             bootstrap_servers = bootstrap_servers,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             auto_offset_reset = "earliest")
    consumer.subscribe([topic_name])

    while True:
        records = consumer.poll(timeout_ms=1.0, max_records=5)
        for k,messages in records.items():
            for message in messages:
                print(message.value)
                
def main():
    topic_name = "org.udacity.crimes.sf.police-calls"
    try:
        asyncio.run(create_consumer(topic_name))
    except KeyboardInterrupt as e:
        print("shutting down")
        
async def create_consumer(topic_name):
    t1 = asyncio.create_task(consume(topic_name))
    await t1
                
if __name__ == "__main__":
    main()