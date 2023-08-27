# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _data_from_clickhouse():
    ch_hook = ClickHouseHook(clickhouse_conn_id='ClickHouse_rnd_conn')
    ch_hook.get_records('select 1;')


with DAG(
        dag_id="upload_data_to_Clickhouse",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=["clickhouse"],
) as dag:
    # [START howto_operator_bash]
    whoami = BashOperator(
        task_id="whoami",
        bash_command="id; echo $?", do_xcom_push=True
    )

    # [START howto_operator_bash]

    install_client = BashOperator(
        task_id="install_client",
        bash_command='curl -o /home/airflow/clickhouse https://clickhouse.com/ |sh; chmod u+x /home/airflow/clickhouse',
        do_xcom_push=True)

    show_client_version = BashOperator(
        task_id="show_client_version",
        bash_command='./clickhouse client --version',
        # run_as_user='airflow',
        # do_xcom_push=True
    )

    get_data_from_clickhouse = PythonOperator(
        task_id='get_data_from_clickhouse',
        python_callable=_data_from_clickhouse,

    )

    whoami >> install_client >> show_client_version >> get_data_from_clickhouse

# if __name__ == "__main__":
#     dag.test()

# clickhouse-client -q "INSERT INTO bi.allevents FORMAT CSV" < 'data_small.csv'
