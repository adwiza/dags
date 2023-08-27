from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


def load_data():
    from clickhouse_driver import Client
    from csv import reader
    settings = {'input_format_null_as_default': True}
    client = Client('10.31.68.81', settings=settings)

    with open('/home/airflow/titanic.csv', 'r') as file:
        csv_reader = reader(file)
        for num, row in enumerate(csv_reader):
            if row and num != 0:
                # row variable is a list that represents a row in csv
                row[2] = row[2].replace('\'', "")
                print(tuple(row))
                values = tuple(row)
                # TODO Change to use the ClickhouseOperator instead client
                client.execute(f"INSERT INTO titanic.data VALUES {values}")


with DAG(
        dag_id='insert_data_to_clickhouse',
        start_date=days_ago(2),
        tags=['titanic', 'clickhouse'],
) as dag:
    drop_table = ClickHouseOperator(
        task_id='drop_table',
        database='titanic',
        sql=('DROP TABLE IF EXISTS titanic.data SYNC',
             ),
        clickhouse_conn_id='ch1',
    )
    create_table = ClickHouseOperator(
        task_id='create_table',
        database='titanic',
        # ['Survived', 'Pclass', 'Name', 'Sex', 'Age', 'Siblings/Spouses Aboard', 'Parents/Children Aboard', 'Fare']
        # ['0', '3', 'Mr. Owen Harris Braund', 'male', '22', '1', '0', '7.25']
        sql=(
            '''
            CREATE TABLE IF NOT EXISTS titanic.data (
            Survived	int,
            Pclass	 int,
            Name	String,
            Sex	    String,
            Age	    float,
            SibSp	int,
            ParChild Int,
            Fare	float
            )
            ENGINE = MergeTree
            ORDER BY Name
            ''',
        ),
        clickhouse_conn_id='ch1',
    )
    insert_data = PythonOperator(
        task_id='insert_data',
        provide_context=True,
        python_callable=load_data,
        # pulling XCom value and printing it
    )

drop_table >> create_table >> insert_data
