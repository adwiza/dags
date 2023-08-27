import json
import time

from elasticsearch import Elasticsearch
from confluent_kafka import Producer
import socket
from config import ES_URL

from loguru import logger

# Initialize Elasticsearch and Kafka connections
es = Elasticsearch([ES_URL])


resp = es.search(index="purchases-000001", size=1000, scroll='10m')

kafka_config = {
    "bootstrap.servers": "10.31.68.81:9092",
    "queue.buffering.max.messages": 100000,  # Adjust as needed
    "queue.buffering.max.ms": 100,  # Adjust as needed
    # Other Kafka configuration options
}

producer = Producer(kafka_config)

old_scroll_id = resp['_scroll_id']

while len(resp['hits']['hits']):

    # # check if there's a new scroll ID
    # if old_scroll_id != resp['_scroll_id']:
    #     logger.info(f"NEW SCROLL ID: {resp['_scroll_id']}")
    #     # pass

    # keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']
    count = 0
    for hit in resp["hits"]["hits"]:
        kafka_message = hit["_source"]
        # Convert dictionary data to JSON and then to bytes
        message_value = json.dumps(kafka_message) #.encode("utf-8")
        # logger.debug(message_value)
        producer.produce("purchases-000001", value=message_value)
        producer.poll(0)  # Trigger delivery reports

        count += 1
        # Check delivery status and implement backpressure if needed
        while producer.flush(1) > 0:  # Wait until all messages are delivered
            time.sleep(1)  # Sleep for a while before checking again
    if count == 1000:
        logger.info(f"{count} records successfully processed")
producer.flush()
