import json
from multiprocessing import Process, cpu_count, Queue

from confluent_kafka import Producer
from elasticsearch import Elasticsearch
from loguru import logger

from config import ES_URL


def produce_messages(queue, kafka_config, topic):
    producer = Producer(kafka_config)

    while True:
        message = queue.get()
        if message is None:
            break

        kafka_message = message["_source"]
        message_value = json.dumps(kafka_message)
        producer.produce(topic, value=message_value)
        producer.poll(0)  # Trigger delivery reports

    producer.flush()


def main():
    es = Elasticsearch([ES_URL])

    resp = es.search(index="purchases-000001", size=10000, scroll='10m')

    kafka_config = {
        "bootstrap.servers": "10.31.68.81:9092",
        "queue.buffering.max.messages": 1000000,  # Adjust as needed
        "queue.buffering.max.ms": 100,  # Adjust as needed
        # Other Kafka configuration options
    }
    kafka_topic = "purchases-000001"

    max_queue_size = 10000
    message_queue = Queue(maxsize=max_queue_size)
    processes = []

    num_processes = cpu_count()  # Number of CPU cores available
    for _ in range(num_processes):
        process = Process(target=produce_messages, args=(message_queue, kafka_config, kafka_topic))
        processes.append(process)
        process.start()

    while len(resp['hits']['hits']):
        old_scroll_id = resp['_scroll_id']
        count = 0

        for hit in resp["hits"]["hits"]:
            message_queue.put(hit)

            count += 1

        if count == 10000:
            logger.info(f"{count} records successfully read from Elastic")

        resp = es.scroll(scroll_id=old_scroll_id, scroll='10m')

    # Signal processes to exit
    for _ in range(num_processes):
        message_queue.put(None)
    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
