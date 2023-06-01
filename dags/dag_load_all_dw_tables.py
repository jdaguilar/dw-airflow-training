#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.sql import SqlSensor
from helpers import cargar_dimension_pais, cargar_dimension_sexo, cargar_dimm_situacion_laboral, cargar_dimm_rango_edad, cargar_dimm_tipo_universidad, cargar_dimm_universidades, cargar_dimm_rama_enseñanza, cargar_dimm_ambito_enseñanza
from dag_stage_egresados_inter import create_dag_for_import as imported_dag_staging_1
from dag_stage_egresados_niveles import create_dag_for_import as imported_dag_staging_2
from dag_stage_egresados_universidad import create_dag_for_import as imported_dag_staging_3
from dag_stage_num_egresados_inter import create_dag_for_import as imported_dag_staging_4
from dag_stage_ramas_conocimiento import create_dag_for_import as imported_dag_staging_5
from dag_stage_situacion_laboral_2 import create_dag_for_import as imported_dag_staging_6
from dag_load_fact_tables import create_dag_for_import as imported_dag_load_fact_tables
import pendulum

with DAG(
    dag_id="dag_load_dw_tables",
    start_date=pendulum.datetime(2023, 5, 20, tz="UTC"),
    schedule_interval="@yearly",
    catchup=False,
    tags=["dw-training"],
    default_args={
        'min_records': 1  # Set the minimum acceptable record count as a parameter
    }
) as dag:
    
    imported_dag_1 = imported_dag_staging_1()
    imported_dag_2 = imported_dag_staging_2()
    imported_dag_3 = imported_dag_staging_3()
    imported_dag_4 = imported_dag_staging_4()
    imported_dag_5 = imported_dag_staging_5()
    imported_dag_6 = imported_dag_staging_6()
    imported_dag_7 = imported_dag_load_fact_tables()
    
    with TaskGroup("Task_group_1", tooltip="Task group for staging tables load") as Task_group_1:
    
        trigger_imp_dag_1 = TriggerDagRunOperator(
            task_id="trigger_imported_dag_staging_1",
            trigger_dag_id=imported_dag_1.dag_id,
            dag=dag,
        )

        trigger_imp_dag_2 = TriggerDagRunOperator(
            task_id=f'trigger_imported_dag_staging_2',
            trigger_dag_id=imported_dag_2.dag_id,
            dag=dag,
        )

        trigger_imp_dag_3 = TriggerDagRunOperator(
            task_id=f'trigger_imported_dag_staging_3',
            trigger_dag_id=imported_dag_3.dag_id,
            dag=dag,
        )

        trigger_imp_dag_4 = TriggerDagRunOperator(
            task_id=f'trigger_imported_dag_staging_4',
            trigger_dag_id=imported_dag_4.dag_id,
            dag=dag,
        )

        trigger_imp_dag_5 = TriggerDagRunOperator(
            task_id=f'trigger_imported_dag_staging_5',
            trigger_dag_id=imported_dag_5.dag_id,
            dag=dag,
        )

        trigger_imp_dag_6 = TriggerDagRunOperator(
            task_id=f'trigger_imported_dag_staging_6',
            trigger_dag_id=imported_dag_6.dag_id,
            dag=dag,
        )

        sensor_task = SqlSensor(
            task_id='check_data_fully_load',
            conn_id='mysql_conn',
            success=lambda data: data > 0,
            sql="SELECT COUNT(*) data FROM stage_situacion_laboral_egresados",
            mode='poke',
            poke_interval=60,
            timeout=3600,
        )

        trigger_imp_dag_1 >> trigger_imp_dag_2 >> trigger_imp_dag_3 >> trigger_imp_dag_4 >> trigger_imp_dag_5 >> trigger_imp_dag_6 >> sensor_task

    with TaskGroup("Task_group_2", tooltip="Task group for dimm tables load") as Task_group_2:

        upload_task_1 = PythonOperator(
            task_id="load_dimm_pais",
            python_callable=cargar_dimension_pais
        )

        upload_task_2 = PythonOperator(
            task_id="load_dimm_sexo",
            python_callable=cargar_dimension_sexo
        )

        upload_task_3 = PythonOperator(
            task_id="load_dimm_situacion_laboral",
            python_callable=cargar_dimm_situacion_laboral
        )

        upload_task_4 = PythonOperator(
            task_id="load_dimm_rango_edad",
            python_callable=cargar_dimm_rango_edad
        )

        upload_task_5 = PythonOperator(
            task_id="load_dimm_tipo_universidad",
            python_callable=cargar_dimm_tipo_universidad
        )

        upload_task_6 = PythonOperator(
            task_id="load_dimm_universidades",
            python_callable=cargar_dimm_universidades
        )

        upload_task_7 = PythonOperator(
            task_id="load_dimm_rama_enseñanza",
            python_callable=cargar_dimm_rama_enseñanza
        )

        upload_task_8 = PythonOperator(
            task_id="load_dimm_ambito_enseñanza",
            python_callable=cargar_dimm_ambito_enseñanza
        )

        [upload_task_1 , upload_task_2 , upload_task_3 , upload_task_4 , upload_task_5 , upload_task_6 , upload_task_7 , upload_task_8]

    with TaskGroup("Task_group_3", tooltip="Task group for fact tables") as Task_group_3:
    
        trigger_imp_dag_7 = TriggerDagRunOperator(
            task_id="trigger_imported_dag_fact_tables",
            trigger_dag_id=imported_dag_7.dag_id,
            dag=dag,
        )

        trigger_imp_dag_7


    Task_group_1 >> Task_group_2 >> Task_group_3


