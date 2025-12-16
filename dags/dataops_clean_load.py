from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id="dataops_clean_load",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dataops"],
) as dag:
    run_job = SSHOperator(
        task_id="run_clean_load_on_spark_client",
        ssh_conn_id="spark_ssh",
        command="""
set -e
cd /opt/repo/repo

echo "=== repo root ==="
ls -la

echo "=== app ==="
ls -la app

python -V
pip -V || true
pip install -r requirements.txt
python app/clean_load.py
""",
        cmd_timeout=60 * 20,
    )