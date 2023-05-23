import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import preprocesar_archivo_situacion_laboral_egresados, cargar_archivo_situacion_laboral_egresados

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = '03003.xlsx'

workflow = DAG(
    "dag_cargar_stage_situacion_laboral",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_situacion_laboral",
        python_callable=preprocesar_archivo_situacion_laboral_egresados,
        op_kwargs=dict(
            file=FILE1,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_situacion_laboral",
        python_callable=cargar_archivo_situacion_laboral_egresados
    )

    preprocessing_task >> upload_task
