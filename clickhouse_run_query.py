from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

with DAG(
    dag_id='clickhouse_operator',
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'clickhouse'],
    catchup=False,
    template_searchpath='$AIRFLOW_HOME/include'
) as dag:

    run_this_last = DummyOperator(task_id='run_this_last')
    # load sql from file
    select_data = ClickhouseOperator(
        task_id='select_quit',
        sql='query.sql',
        click_conn_id='my_clickhouse_connection',
        do_xcom_push=False
    )
    # set sql direct in the code section
    select_push = ClickhouseOperator(
        task_id='select_xcom',
        sql='select * from TestTable;;',
        click_conn_id='my_clickhouse_connection'
    )

    select_data >> select_push >> run_this_last