# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="install_elastic",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["elasticsearch", "ELK"],
) as dag:

    # [START howto_operator_bash]
    cat_pip_freeze = BashOperator(
        task_id="cat_pip_freeze",
        bash_command="pip freeze > requirements.txt; cat requirements.txt; python --version",
        do_xcom_push=True
    )
    # [END howto_operator_bash]

    install_packages = BashOperator(
        task_id="install_packages",
        bash_command='pip install --upgrade pip --user; pip install -U elasticsearch apache-airflow \
                     airflow-clickhouse-plugin[pandas] pandas numpy --user; \
                     pip install -U airflow-clickhouse-plugin --user'
    )

    install_packages >> cat_pip_freeze


if __name__ == "__main__":
    dag.test()