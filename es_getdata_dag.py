from __future__ import annotations

from datetime import datetime
from elasticsearch import Elasticsearch, exceptions
import json
import os
import pandas as pd
import time
from config import ES_URL

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook
from loguru import logger

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "get_elasticsearch_data"
CONN_ID = "es1"


@task(task_id="es_get_all_data")
def show_data():
    # create a timestamp using the time() method
    start_time = time.time()

    # declare globals for the Elasticsearch client host
    DOMAIN = ES_URL
    PORT = 443

    # concatenate a string for the client's host paramater
    host = str(DOMAIN) + ":" + str(PORT)

    # declare an instance of the Elasticsearch library
    client = Elasticsearch(host)

    try:
        time.sleep(3)
        # use the JSON library's dump() method for indentation
        info = json.dumps(client.info(), indent=4)
        # pass client object to info() method
        print("Elasticsearch client info():", info)

    except exceptions.ConnectionError as err:

        # print ConnectionError for Elasticsearch
        logger.exception('Canceled fetching {err}')

        logger.error(f"Elasticsearch info() ERROR: {err}")
        logger.error(f"The client host: {host}, is invalid or cluster is not running")

        # change the client's value to 'None' if ConnectionError
        client = None

    # valid client instance for Elasticsearch
    if client is not None:

        # get all of the indices on the Elasticsearch cluster
        all_indices = client.indices.get_alias(
            index=['transactions-000000'])  # purchases-000001 transactions-000000
        # print(json.dumps(all_indices, indent=4))
        # keep track of the number of the documents returned
        doc_count = 0

        # iterate over the list of Elasticsearch indices
        for num, index in enumerate(all_indices):
            # print(num, index)

            # make a search() request to get all docs in the index
            resp = client.search(
                index=index,
                scroll='1m'  # length of time to keep search context
            )

            # keep track of pass scroll _id
            old_scroll_id = resp['_scroll_id']
            # create dataframe
            data = pd.DataFrame()
            # use a 'while' iterator to loop over document 'hits'
            while len(resp['hits']['hits']):

                # make a request using the Scroll API
                resp = client.scroll(
                    scroll_id=old_scroll_id,
                    scroll='1m'  # length of time to keep search context
                )

                # check if there's a new scroll ID
                if old_scroll_id != resp['_scroll_id']:
                    # logger.info(f"NEW SCROLL ID: {resp['_scroll_id']}")
                    pass

                # keep track of pass scroll _id
                old_scroll_id = resp['_scroll_id']

                # print the response results
                # logger.info(f"response for index: {index}")
                # logger.info(f"_scroll_id: {resp['_scroll_id']}")
                # logger.info(f'response["hits"]["total"]["value"]: {resp["hits"]["total"]["value"]}')

                # iterate over the document hits for each 'scroll'
                for doc in resp['hits']['hits']:
                    # create a Series object from doc dict object
                    doc_data = pd.Series(doc['_source'], name=doc['_id'])
                    # append the Series object to the DataFrame object
                    # data = data._append(doc_data)
                    data = pd.concat([data, doc_data])
                    # logger.info(f"{doc['_id']} {doc['_source']}")

                    doc_count += 1
                    # logger.info(f"DOC COUNT: {doc_count}")
                if doc_count % 200000 == 0:
                    # logger.info(f"File {'data-' + index + '-' + str(doc_count) + '.csv'} has successfully written.")
                    logger.info(
                        f"File {'data-' + index + '-' + str(doc_count) + '.parquet.gzip'} has successfully written.")
                    # data.to_csv('data-' + index + '-' + str(doc_count) + '.csv')
                    data.to_parquet('data-' + index + '-' + str(doc_count) + '.parquet.gzip', compression='gzip')
                    if not data.empty:
                        data = pd.DataFrame()
                        doc_data = pd.Series(dtype='object')
            # data.to_csv('data-' + index + '-' + str(doc_count) + '.csv')
            data.to_parquet('data-' + index + '-' + str(doc_count) + '.parquet.gzip', compression='gzip')
        # print the total time and document count at the end
        logger.info(f"\nTOTAL DOC COUNT: {doc_count}")

    # print the elapsed time
    logger.info(f"TOTAL TIME:, {time.time()} - {start_time}, seconds.")


with models.DAG(
        DAG_ID,
        schedule="@once",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["ELK", "elasticsearch"],
) as dag:
    show_data()
    #
    # es_python_test = PythonOperator(
    #     task_id="print_data_from_elasticsearch", python_callable=show_data
    # )
