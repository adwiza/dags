from logging import getLogger

from airflow import DAG
from de_airflow_plugins.operators.spark_submit_operator import SparkSubmitCustomOperator
from de_airflow_plugins.mattermost.mattermost_notification import dag_failed_mattermost_notification

from config.spark_operator_base_config import SPARK_BASE_OPERATOR_DEFAULTS_CONFIG, SPARK_BASE_POD_CONFIG
from config.dag_config import validate_and_get_json_dag_config, application_args_from_dict

logger = getLogger(__name__)

DAG_NAME: str = "sample_spark_submit_operator_dwh_cmd_ver4"
DAG_BASE_CONFIG = validate_and_get_json_dag_config(DAG_NAME)
execution_date: str = "{{ ds }}"


airflow_var_example = {
    "dag_config": {
        "owner": "adwiz",
        "start_date": "2023-04-17",
        "schedule_interval": "16 00 * * *",
        "tags": ["de", "sample"],
        "exec_config": {
            "de_make_amazme_likes": {
                "num_executors": "10",
                "executor_cores": "2",
                "driver_memory": "4g",
                "executor_memory": "4g",
            },
            # optional: applies on all tasks if task_name is not specified
            "defaults": {"num_executors": "1", "executor_cores": "1", "driver_memory": "1g", "executor_memory": "1g"},
        },
    }
}


SPARK_BASE_OPERATOR_DEFAULTS_CONFIG_TEST = {
    "yarn_conn": "yarn",
    "conn_id": "spark_default",
    "executor_config": SPARK_BASE_POD_CONFIG,
    "env_vars": {"PYSPARK_PYTHON": "./pyspark-submit-env.pex", "PEX_ROOT": "hdfs:///tmp/pex-tmp"},
}

with DAG(
    dag_id=DAG_NAME,
    # description=str(DAG_BASE_CONFIG.read_dag_config_docs(dag_name=DAG_NAME)),
    # doc_md=DAG_BASE_CONFIG.render_doc_md(DAG_NAME),
    on_failure_callback=dag_failed_mattermost_notification,
    **DAG_BASE_CONFIG.merge_default_and_airflow_dag_config(),
) as dag:
    task_name: str = "de_make_amazme_likes"

    spark_submit_operator = SparkSubmitCustomOperator(
        task_id=task_name,
        name=task_name,
        application=DAG_BASE_CONFIG.get_application_runner("transformation_runner.py"),
        application_args=application_args_from_dict(dict(job_name=task_name, execution_date=execution_date)),
        files=DAG_BASE_CONFIG.get_pex_files(),
        # spark config
        conf={
            **DAG_BASE_CONFIG.task_spark_configs(),
            # **{
            #     "spark.executorEnv.PEX_ROOT": "hdfs:///tmp/pex-tmp",
            #     "spark.yarn.appMasterEnv.PEX_ROOT": "hdfs:///tmp/pex-tmp",
            # },
        },
        # extra
        verbose=True,
        # jars="some.jars",
        # status_poll_interval=5, # default = 1 sec
        # defaults
        **SPARK_BASE_OPERATOR_DEFAULTS_CONFIG,
        # spark config from airflow/json
        **DAG_BASE_CONFIG.get_task_exec_config_as_dict(),
    )