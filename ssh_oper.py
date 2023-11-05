from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

from airflow.utils.dates import days_ago
from ipaddress import IPv4Network

sshHook = SSHHook(ssh_conn_id="ssh1", cmd_timeout=60)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'vacuum_journal_1_day',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
)

# Define the list of IP addresses within the subnet
ip_subnet = IPv4Network('10.31.68.64/26')
ip_addresses = [str(ip) for ip in ip_subnet.hosts()]

# Define the SSHOperator tasks for each IP address
ssh_conn_id = 'ssh1'  # Replace with your SSH connection ID configured in Airflow
command = 'sudo journalctl --vacuum-time=1d'  # Replace with the command you want to run on the remote host

ssh_tasks = []
for ip_address in ip_addresses:
    task_id = f'run_command_{ip_address.replace(".", "_")}'
    ssh_task = SSHOperator(
        task_id=task_id,
        ssh_conn_id=ssh_conn_id,
        command=command,
        remote_host=ip_address,  # Specify the remote host for the SSH connection
        do_xcom_push=True,  # Push the output of the command to XCom
        dag=dag,
    )
    ssh_tasks.append(ssh_task)

