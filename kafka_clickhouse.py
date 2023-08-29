import json
from confluent_kafka import Consumer
from loguru import logger

consumer_config = {
    'bootstrap.servers': '10.31.68.81:9092',  # Replace with actual Kafka broker
    'group.id': '1001',
    'auto.offset.reset': 'earliest',  # Change to 'latest' if needed
}

consumer = Consumer(consumer_config)
consumer.subscribe(['purchases-000001'])  # Replace with your Kafka topic

columns = [
    'id', 'timestamp', 'category_id', 'category_name', 'clip_id', 'clip_added_at',
    'customer_id', 'customer_email', 'customer_country', 'customer_logged_in',
    'discount_cid', 'discount_description' 'price_usd', 'studio_id', 'studio_name',
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

try:
    while True:
        message = consumer.poll(1.0)  # Poll for messages with a timeout
        if message is None:
            continue
        if message.error():
            logger.error("Error: {}".format(message.error()))
            continue
        # Process and transform message value
        decoded_value = message.value().decode("utf-8")
        # Parse the decoded value as a JSON object
        json_data = json.loads(decoded_value)
        flattened_dict = flatten_dict(json_data)

        # Extract values corresponding to columns' order
        values = [flattened_dict.get(key, "Empty value") for key in columns]

        # Print keys and values as separate strings
        keys_string = ', '.join(columns)
        cols_string = ', '.join(columns)
        values_string = ', '.join(map(str, values))  # Convert values to strings

        print("Keys:", keys_string)
        print("Cols:", cols_string)
        print("Values:", values_string)
        print(len(columns))
        print(len(values))
        print('#####' * 100)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
