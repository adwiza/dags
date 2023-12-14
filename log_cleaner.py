from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
from ipaddress import IPv4Network

# sshHook = SSHHook(ssh_conn_id="ssh2", cmd_timeout=60)

default_args = {
    'owner': 'adwiz',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1),
    'retry_delay': timedelta(minutes=2),
    'retries': 1,
}

dag = DAG(
    'log_cleaner_3days',
    default_args=default_args,
    schedule_interval='0 5 * * 1,6',
    tags=['log_cleaner'],
    # render_template_as_native_obj=True,
    catchup=False
)

# Define the SSHOperator tasks for each IP address
ssh_conn_id = 'ssh2'  # Replace with your SSH connection ID configured in Airflow
ip_address = '10.25.162.18'  # '216.18.162.173'


ssh_task = SSHOperator(
    task_id='log_cleaner_3days',
    # ssh_hook=sshHook,
    ssh_conn_id=ssh_conn_id,
    command='sudo log_cleaner',
    remote_host=ip_address,  # Specify the remote host for the SSH connection
    do_xcom_push=True,  # Push the output of the command to XCom
    dag=dag,
)