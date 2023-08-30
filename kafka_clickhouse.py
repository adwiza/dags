import concurrent
import json
import threading

from clickhouse_driver.errors import ServerException
from confluent_kafka import Consumer
from loguru import logger
from clickhouse_driver import Client

consumer_config = {
    'bootstrap.servers': '10.31.68.81:9092',  # Replace with actual Kafka broker
    'group.id': '1001',
    'auto.offset.reset': 'earliest',  # Change to 'latest' if needed
}

consumer = Consumer(consumer_config)
consumer.subscribe(['purchases-000001'])  # Replace with your Kafka topic

clickhouse_config = {
    'host': '10.31.68.81',  # Replace with actual ClickHouse host
    'port': 9000
}
clickhouse_client = Client(**clickhouse_config)

columns = [
    'id', 'timestamp', 'category_id', 'category_name', 'clip_id', 'clip_added_at',
    'customer_id', 'customer_email', 'customer_country', 'customer_logged_in',
    'discount_cid', 'discount_description', 'price_usd', 'studio_id', 'studio_name',
    'studio_payout', 'meta_processed_at', 'meta_origin_processed_at',
    'transaction_id', 'initial_transaction_days_from', 'initial_transaction_timestamp',
    'previous_transaction_days_from', 'oneclick', 'payment_method', 'currency'
]


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


batch_size = 100000
batch_values = []
lock = threading.Lock()


def insert_batch(clickhouse_client, columns, batch_values):
    values_string = ', '.join(batch_values)
    query = f"INSERT INTO purchases.data ({', '.join(columns)}) VALUES {values_string}"

    try:
        clickhouse_client.execute(query)
        logger.info(f"Inserted {len(batch_values)} records into ClickHouse")
    except ServerException.code as e:
        logger.error("Error inserting data into ClickHouse:", e)


def process_message(message):
    decoded_value = message.value().decode("utf-8")
    json_data = json.loads(decoded_value)
    flattened_dict = flatten_dict(json_data)

    values = ["'" + str(flattened_dict.get(key, "Empty value")).replace("'", "") + "'" for key in columns]
    with lock:
        batch_values.append('(' + ', '.join(values) + ')')

    if len(batch_values) >= batch_size:
        with lock:
            insert_batch(clickhouse_client, columns, batch_values)
            batch_values.clear()


try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Error: {}".format(message.error()))
                continue

            executor.submit(process_message, message)

except KeyboardInterrupt:
    pass
finally:
    if batch_values:
        with lock:
            insert_batch(clickhouse_client, columns, batch_values)

    consumer.close()

# `id` Int,
# `timestamp` UInt32,
# `category_id` Int,
# `category_name` String,
# `clip_id` Int,
# `clip_added_at` UInt32,
# `customer_id` Int,
# `customer_email` String,
# `customer_country` String,
# `customer_logged_in` Bool,
# `discount_cid` Int,
# `discount_description` Int,
# `price_usd` String,
# `studio_id` Int,
# `studio_name` String,
# `studio_payout` Float,
# `meta_processed_at` UInt32,
# `meta_origin_processed_at` UInt32,
# `transaction_id` Int,
# `initial_transaction_days_from` Int,
# `initial_transaction_timestamp` UInt32,
# `previous_transaction_days_from` Int,
# `oneclick` Bool,
# `payment_method` String,
# `currency` String


# `id` String,
# `timestamp` String,
# `category_id` String,
# `category_name` String,
# `clip_id` String,
# `clip_added_at` String,
# `customer_id` String,
# `customer_email` String,
# `customer_country` String,
# `customer_logged_in` String,
# `discount_cid` String,
# `discount_description` String,
# `price_usd` String,
# `studio_id` String,
# `studio_name` String,
# `studio_payout` String,
# `meta_processed_at` String,
# `meta_origin_processed_at` String,
# `transaction_id` String,
# `initial_transaction_days_from` String,
# `initial_transaction_timestamp` String,
# `previous_transaction_days_from` String,
# `oneclick` String,
# `payment_method` String,
# `currency` String
