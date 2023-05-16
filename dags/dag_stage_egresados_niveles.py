import os

from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from helpers import preprocesar_archivo_situacion_laboral, cargar_archivo_situacion_laboral

import staging  


#import data.processed.stagings as stagings


DATA_DIRECTORY = "/tmp/data/raw/"
FILE = '03003.xlsx'


workflow = DAG(
    "dag_staging",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

with workflow:

    staging_tables = PythonOperator(
        task_id="insertar_data_stagings",
        python_callable=staging.main_staging
        # op_kwargs=dict(
        #     file=FILE,
        #     directory=DATA_DIRECTORY,
        # ),
    )

    # upload_task = PythonOperator(
    #     task_id="cargar_archivo_situacion_laboral_egresados",
    #     python_callable=cargar_archivo_situacion_laboral,
        # op_kwargs=dict(
        #     user=MYSQL_USER,
        #     password=MYSQL_PASSWORD,
        #     host=MYSQL_HOST,
        #     port=MYSQL_PORT,
        #     db=MYSQL_DATABASE,
        #     table_name=TABLE_NAME_TEMPLATE,
        #     csv_file=OUTPUT_FILE_TEMPLATE
        # ),
    #)

    staging_tables 
