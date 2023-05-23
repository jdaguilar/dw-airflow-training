import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import preprocesar_archivo_ramas_conocimiento, cargar_archivo_ramas_conocimiento

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = 'ISCED_2013.csv'

workflow = DAG(
    "dag_cargar_stage_ramas_conocimiento",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_ramas_conocimiento",
        python_callable=preprocesar_archivo_ramas_conocimiento,
        op_kwargs=dict(
            file=FILE1,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_ramas_conocimiento",
        python_callable=cargar_archivo_ramas_conocimiento
    )

    preprocessing_task >> upload_task
