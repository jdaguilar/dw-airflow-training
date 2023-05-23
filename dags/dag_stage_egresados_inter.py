import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import cargar_archivo_egresados_internacional, preprocesar_archivo_egresados_internacional

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = 'educ_uoe_grad05.xlsx'

workflow = DAG(
    "dag_cargar_stage_egresados_internacional_V1",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_egresados_internacional",
        python_callable=preprocesar_archivo_egresados_internacional,
        op_kwargs=dict(
            file=FILE1,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_egresados_internacional",
        python_callable=cargar_archivo_egresados_internacional
    )

    preprocessing_task >> upload_task
