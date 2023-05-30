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
from helpers import load_fact_international_graduated, load_fact_egresados_rama_ensenanza, load_fact_egresados_niveles, load_fact_situacion_laboral_egresados


def create_dag_for_import():
        
    workflow = DAG(
        "dag_cargar_fact_tables",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training'],
    )

    with workflow:

        task_load_fact_international_graduated = PythonOperator(
            task_id="task_load_fact_international_graduated",
            python_callable=load_fact_international_graduated
        )

        task_load_fact_egresados_rama_ensenanza = PythonOperator(
            task_id="task_load_fact_egresados_rama_ensenanza",
            python_callable=load_fact_egresados_rama_ensenanza
        )

        task_load_fact_egresados_niveles = PythonOperator(
            task_id="task_load_fact_egresados_niveles",
            python_callable=load_fact_egresados_niveles
        )

        
        task_load_fact_situacion_laboral_egresados = PythonOperator(
            task_id="task_load_fact_situacion_laboral_egresados",
            python_callable=load_fact_situacion_laboral_egresados
        )

        [task_load_fact_international_graduated , task_load_fact_egresados_rama_ensenanza , task_load_fact_egresados_niveles , task_load_fact_situacion_laboral_egresados]
            
    return workflow

dag = create_dag_for_import()