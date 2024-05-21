from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

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
            export DBNAME_REDSHIFT={{ var.value.DBNAME_REDSHIFT }} && \
            export SCHEMA_NAME_REDSHIFT={{ var.value.SCHEMA_NAME_REDSHIFT }} && \
            export USER_REDSHIFT={{ var.value.USER_REDSHIFT }} && \
            export PASS_REDSHIFT={{ var.value.PASSWORD_REDSHIFT }} && \
            export HOST_REDSHIFT={{ var.value.HOST_REDSHIFT }} && \
            export PORT_REDSHIFT={{ var.value.PORT_REDSHIFT }} && \
            export TABLE_NAME_REDSHIFT={{ var.value.TABLE_NAME_REDSHIFT }} && \
            export DS_DATE={{ ds }} && \
            export DS_TOMORROW={{ macros.ds_add(ds, 1) }} && \
            python /opt/airflow/python_scripts/script_Belossi_Pre_Entrega3.py
        '''
    )

    start >> task1 >> end
