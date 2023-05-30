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
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from helpers import preprocesar_archivo_ramas_conocimiento, cargar_archivo_ramas_conocimiento
from datetime import timedelta

DATA_DIRECTORY = "/tmp/data/raw/"
FILE1 = 'ISCED_2013.csv'

def create_dag_for_import():

    workflow = DAG(
        "dag_cargar_stage_ramas_conocimiento",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training'],
        dagrun_timeout  = timedelta(minutes=2)
    )

    with workflow:

        borrar_tabla_mysql = MySqlOperator(
            task_id='borrar_tabla_dw_stage_ramas_conocimiento',
            mysql_conn_id='mysql_conn', 
            sql='DELETE FROM stage_ramas_conocimiento'
        )

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

        borrar_tabla_mysql >> preprocessing_task >> upload_task

    return workflow

dag = create_dag_for_import()