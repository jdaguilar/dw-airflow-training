import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import preprocesar_archivo_egresados_universidad, cargar_archivo_egresados_universidad

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = 'SEGR1.csv'
FILE2 = 'SEGR2.csv'


workflow = DAG(
    "dag_cargar_stage_egresados_universidad",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_egresados_universidad",
        python_callable=preprocesar_archivo_egresados_universidad,
        op_kwargs=dict(
            file1=FILE1,
            file2=FILE2,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_egresados_universidad",
        python_callable=cargar_archivo_egresados_universidad
    )

    preprocessing_task >> upload_task
