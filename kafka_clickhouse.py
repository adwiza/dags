import json

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from confluent_kafka import Consumer
from loguru import logger

consumer_config = {
    'bootstrap.servers': '10.31.68.81:9092',  # Replace with actual Kafka broker
    'group.id': '1001',
    'auto.offset.reset': 'earliest',  # Change to 'latest' if needed
}

consumer = Consumer(consumer_config)
consumer.subscribe(['purchases-000001'])  # Replace with your Kafka topic

clickhouse_client = Client(host='10.31.68.81', port=9000)  # Replace with actual ClickHouse host


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


try:
    while True:
        message = consumer.poll(1.0)  # Poll for messages with a timeout
        if message is None:
            continue
        if message.error():
            print("Error: {}".format(message.error()))
            continue
        # Process and transform message value
        decoded_value = message.value().decode("utf-8")
        # Parse the decoded value as a JSON object
        json_data = json.loads(decoded_value)
        flattened_dict = flatten_dict(json_data)
        key_tuples = tuple(f"{key}" for key, value in flattened_dict.items())
        value_tuples = tuple(f"{value}" for key, value in flattened_dict.items())
        # print(key_tuples)
        # print(value_tuples)
        # logger.info(key_tuples)
        # logger.info(value_tuples)
        # Insert data into ClickHouse
        try:
            clickhouse_client.execute(f"INSERT INTO purchases.data VALUES {value_tuples}")
        except ServerException as err:
            # print(key_tuples)
            # print(value_tuples)
            logger.error(err)
            # print(key_tuples)
            # print(value_tuples)
            # print(err)


except KeyboardInterrupt:
    pass
finally:
    consumer.close()

# try:
#     batch_size = 1000
#     records = []  # Accumulate records to be inserted
#
#     while True:
#         message = consumer.poll(1.0)  # Poll for messages with a timeout
#         if message is None:
#             continue
#         if message.error():
#             print("Error: {}".format(message.error()))
#             continue
#
#         # Process and transform message value
#         decoded_value = message.value().decode("utf-8")
#         json_data = json.loads(decoded_value)
#         flattened_dict = flatten_dict(json_data)
#         key_value_tuples = tuple(value for key, value in flattened_dict.items())
#         records.append(key_value_tuples)
#
#         if len(records) >= batch_size:
#             batch = ','.join('(' + ', '.join(f"'{value}'" for value in record) + ')' for record in records)
#             query = f"INSERT INTO purchases.data VALUES {batch}"
#             try:
#                 clickhouse_client.execute(query)
#             except ServerException as err:
#                 print(err)
#             logger.info(f"Inserted {batch_size} records into ClickHouse")
#             records = []  # Clear the batch for the next set of records
#
# except KeyboardInterrupt:
#     pass
# finally:
#     if records:
#         batch = ','.join('(' + ', '.join(f"'{value}'" for value in record) + ')' for record in records)
#         query = f"INSERT INTO purchases.data VALUES {batch}"
#         try:
#             clickhouse_client.execute(query)
#         except ServerException as err:
#             print(err)
#         logger.info(f"Inserted {len(records)} records into ClickHouse")
#     consumer.close()
