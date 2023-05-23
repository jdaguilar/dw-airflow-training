import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import cargar_archivo_egresados_niveles, preprocesar_archivo_egresados_niveles

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = 'grad_5sc.csv'

workflow = DAG(
    "dag_cargar_stage_egresados_niveles",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_egresados_niveles",
        python_callable=preprocesar_archivo_egresados_niveles,
        op_kwargs=dict(
            file=FILE1,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_egresados_niveles",
        python_callable=cargar_archivo_egresados_niveles
    )

    preprocessing_task >> upload_task
