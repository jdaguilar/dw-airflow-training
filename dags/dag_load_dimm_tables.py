import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import cargar_dimension_pais, cargar_dimension_sexo, cargar_dimm_situacion_laboral, cargar_dimm_rango_edad, cargar_dimm_tipo_universidad, cargar_dimm_universidades, cargar_dimm_rama_enseñanza, cargar_dimm_ambito_enseñanza


workflow = DAG(
    "dag_cargar_tablas_dimensiones",
    schedule_interval="@yearly",
    start_date=datetime(2023, 5, 20),
    tags=['dw-training'],
)

with workflow:

    upload_task_1 = PythonOperator(
        task_id="cargar_tabla_dimm_pais",
        python_callable=cargar_dimension_pais
    )

    upload_task_2 = PythonOperator(
        task_id="cargar_tabla_dimm_sexo",
        python_callable=cargar_dimension_sexo
    )

    upload_task_3 = PythonOperator(
        task_id="cargar_dimm_situacion_laboral",
        python_callable=cargar_dimm_situacion_laboral
    )

    upload_task_4 = PythonOperator(
        task_id="cargar_dimm_rango_edad",
        python_callable=cargar_dimm_rango_edad
    )

    upload_task_5 = PythonOperator(
        task_id="cargar_dimm_tipo_universidad",
        python_callable=cargar_dimm_tipo_universidad
    )

    upload_task_6 = PythonOperator(
        task_id="cargar_dimm_universidades",
        python_callable=cargar_dimm_universidades
    )

    upload_task_7 = PythonOperator(
        task_id="cargar_dimm_rama_enseñanza",
        python_callable=cargar_dimm_rama_enseñanza
    )

    upload_task_8 = PythonOperator(
        task_id="cargar_dimm_ambito_enseñanza",
        python_callable=cargar_dimm_ambito_enseñanza
    )

[upload_task_1 , upload_task_2, upload_task_3, upload_task_4, upload_task_5, upload_task_6, upload_task_7, upload_task_8]
