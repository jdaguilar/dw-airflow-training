import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from helpers import preprocesar_archivo_num_egresados_inter, cargar_archivo_num_egresados_inter

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = 'educ_uoe_grad01.xlsx'

workflow = DAG(
    "dag_cargar_stage_num_egresados_inter",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    borrar_tabla_mysql = MySqlOperator(
        task_id='borrar_tabla_dw',
        mysql_conn_id='mysql_conn', 
        sql='DELETE FROM stage_numero_egresados_internacional'
    )

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_num_egresados_inter",
        python_callable=preprocesar_archivo_num_egresados_inter,
        op_kwargs=dict(
            file=FILE1,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_num_egresados_inter",
        python_callable=cargar_archivo_num_egresados_inter
    )

    borrar_tabla_mysql >> preprocessing_task >> upload_task
