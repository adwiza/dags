from typing import Dict, List, Any
import logging
import ast

from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from de_airflow_plugins.operators.presto_safe_get_records_operator import PrestoSafeGetRecordsOperator
from de_airflow_plugins.presto_stats import sql_list_tables
from config.config_reader import create_default_args
from de_airflow_plugins.spark_submit_config import ExecutionParameters, make_spark_submit
from de_airflow_plugins.mattermost.mattermost_notification import dag_failed_mattermost_notification

DAG_NAME: str = "de_ingestion_hive_to_clickhouse"
default_args: Dict = create_default_args(dag_name=DAG_NAME)
execution_Date: str = "{{ ds }}"
workers_num: int = 8

schemas_mapping = {
    "data_mart_analytics": {
        "target_schema_name": "data_mart_analytics",
        "exclude_tables": [
            "business_stickiness_auth_retention_reg",
            "licensing_streams_reg",
            "business_stickiness_daysofweek_distr_reg",
            "business_stickiness_multiplatform_users_reg",
            "business_stickiness_sberprime_dau_mau_reg",
            "business_stickiness_sberprime_dau_wau_reg",
            "business_stickiness_track_count_w_roll_reg",
            "jira_analytics_task_changelog_reg",
            "jira_analytics_task_dict_reg",
            "performance_metrics_installs_1d_raw_d_reg",
            "performance_metrics_installs_1d_subscriptions_raw_d_reg",
            "performance_metrics_installs_7d_raw_d_reg",
            "performance_metrics_installs_7d_subscriptions_raw_d_reg",
            "popular_personal_profiles_m_reg",
            "product_statistics_sections_first_play30_day_reg",
            "product_statistics_sections_play30_day_reg",
            "product_statistics_self_content_interaction_daily_reg",
            "product_statistics_self_main_interaction_daily_reg",
            "product_statistics_self_main_interaction_monthly_reg",
            "product_statistics_self_main_interaction_weekly_reg",
            "subscription_info_sales_report_reg",
            "user_info_platform_first_visit_reg",
        ],
    }
}


def split_into_groups(records: Any) -> List[str]:
    """Result[i] = group_i = str 'schema_hive1,schema_ch1,table_name1; ... ;schema_hiveN,schema_chN,table_nameN'"""

    if not records or len(records) == 0:
        logging.info("The tables list is empty.")
        return ["" for _ in range(workers_num)]

    if isinstance(records[0], str):
        records = ast.literal_eval(records)

    hive_tables: List[str] = [rec[0] for rec in records]
    logging.info(f"Found {len(hive_tables)} hive tables.")

    groups: List[List[str]] = [[] for _ in range(workers_num)]

    for schema_hive, schema_clickhouse in schemas_mapping.items():
        names: List[str] = [name.split(".")[1] for name in hive_tables if name.split(".")[0] == schema_hive]
        names: List[str] = list(set(names) - set(schema_clickhouse["exclude_tables"]))
        schema_ch: str = schema_clickhouse["target_schema_name"]

        for i in range(workers_num):
            groups[i] += [f"{schema_hive},{schema_ch},{table}" for table in names[i::workers_num]]

    for i, gr in enumerate(groups):
        logging.info(f"Group {i} contains {len(gr)} tables.")

    return [";".join(gr) for gr in groups]


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=default_args["start_date"],
    schedule_interval=default_args["schedule_interval"],
    max_active_runs=default_args["max_active_runs"],
    tags=default_args["tags"],
    doc_md=default_args["md_docs"],
    on_failure_callback=dag_failed_mattermost_notification,
) as dag:
    get_tables_list: PrestoSafeGetRecordsOperator = PrestoSafeGetRecordsOperator(
        task_id="get_tables_list", sql=sql_list_tables
    )

    prepare_workers: PythonOperator = PythonOperator(
        task_id="prepare_workers",
        python_callable=split_into_groups,
        op_kwargs=dict(records="{{ ti.xcom_pull(task_ids='get_tables_list') }}"),
        do_xcom_push=True,
    )

    for i in range(workers_num):
        ingestion_hive_to_clickhouse = SSHOperator(
            task_id=f"{DAG_NAME}_{i}",
            ssh_conn_id="ssh-hdfs",
            remote_host=BaseHook.get_connection("ssh-hdfs").host,
            command=make_spark_submit(
                name=f"{DAG_NAME}_{i}",
                queue=Variable.get("env_queue"),
                execution_parameters=ExecutionParameters(**default_args["params"][DAG_NAME]["exec_config"]),
                job_runner=default_args["ingestion_runner"],
                py_files=default_args["code_base"],
                job_args=dict(
                    job_name=f"de_ingestion_hive_to_clickhouse",
                    source=f"{{{{ ti.xcom_pull(task_ids='prepare_workers').{i} }}}}",
                    execution_date=execution_Date,
                    s3_env=Variable.get("s3_env"),
                ),
            ),
        )

        prepare_workers >> ingestion_hive_to_clickhouse

    get_tables_list >> prepare_workers