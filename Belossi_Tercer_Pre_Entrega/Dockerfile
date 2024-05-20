FROM apache/airflow:2.9.1-python3.8

USER root
COPY requirements.txt requirements.txt
USER airflow

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/airflow

COPY python_scripts /opt/airflow/python_scripts
COPY dags /opt/airflow/dags
COPY logs /opt/airflow/logs
COPY plugins /opt/airflow/plugins
COPY binance /opt/airflow/binance




