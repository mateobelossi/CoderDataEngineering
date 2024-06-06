#!/bin/bash

airflow db init

airflow users create \
    --username airflow \
    --password airflow \
    --firstname Mateo \
    --lastname Belossi \
    --role Admin \
    --email mateo.belossi@bbsoft.us

airflow scheduler &
exec airflow webserver