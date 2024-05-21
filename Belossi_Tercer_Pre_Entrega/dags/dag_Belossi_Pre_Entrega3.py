from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

#PASSWORD_REDSHIFT = Variable.get("PASSWORD_REDSHIFT")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 20),
    'retries': 1,
}

with DAG(
    dag_id = 'Belossi_Pre_Entrega3',
    description='ETL: Get Binance Coins values, clean & transform, and finally load to Redshift.',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval= "15 00 * * *",
    tags=["binance_coins"],
    catchup=False,
) as dag:
    start = DummyOperator(
            task_id=f"start",
            )

    end = DummyOperator(
            task_id=f"end",
            )
    
    task1 = BashOperator(
        task_id='get_binance_coins_values',
        bash_command='''\
            export DBNAME_REDSHIFT="data-engineer-database" && \
            export SCHEMA_NAME_REDSHIFT="mateobelossi_coderhouse" && \
            export USER_REDSHIFT="mateobelossi_coderhouse" && \
            export PASS_REDSHIFT="{{ var.value.PASSWORD_REDSHIFT }}" && \
            export HOST_REDSHIFT="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com" && \
            export PORT_REDSHIFT="5439" && \
            export TABLE_NAME_REDSHIFT="binance_coins" && \
            export DS_DATE="{{ ds }}" && \
            export DS_TOMORROW="{{ macros.ds_add(ds, 1) }}" && \
            python /opt/airflow/python_scripts/script_Belossi_Pre_Entrega3.py
        ''',
    )

    start >> task1 >> end
