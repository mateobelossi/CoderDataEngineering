FROM python:3.9.5-slim-buster

RUN groupadd -g 50000 airflow && \
    useradd -u 50000 -g airflow -m -s /bin/bash airflow

RUN pip install apache-airflow

WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY python_scripts /opt/airflow/python_scripts

COPY dags /opt/airflow/dags

COPY logs /opt/airflow/logs

COPY plugins /opt/airflow/plugins

COPY entrypoint.sh /entrypoint.sh

COPY alerts /opt/airflow/alerts

USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]

EXPOSE 8080 8793

CMD ["webserver"]



