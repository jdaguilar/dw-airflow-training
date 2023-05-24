import pandas as pd
import numpy as np
from airflow.hooks.mysql_hook import MySqlHook
import logging

def cargar_dimension_pais():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    query = "SELECT nombre_pais FROM dw.dimm_pais"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_pais_db = pd.DataFrame(results, columns=['nombre_pais'])
    else:
        df_dim_pais_db = pd.DataFrame(columns=['nombre_pais'])

    df_dim_pais_db = df_dim_pais_db.rename(columns={'nombre_pais': 'nombre_pais_db'})

    # read stage_porcentaje_egresados_internacional
    query = "SELECT distinct country FROM dw.stage_porcentaje_egresados_internacional"
    results = mysql_hook.get_records(sql=query)
    
    stage_porcentaje_egresados_internacional = pd.DataFrame(results, columns=['country'])

    df_dim_pais = stage_porcentaje_egresados_internacional[['country']].drop_duplicates().reset_index(drop=True)
    df_dim_pais = df_dim_pais.rename(columns={'country': 'nombre_pais'})

    nuevos_paises = pd.merge(df_dim_pais, df_dim_pais_db,left_on='nombre_pais', right_on='nombre_pais_db', how='left')
    nuevos_paises = nuevos_paises[nuevos_paises['nombre_pais_db'].isnull()].drop(['nombre_pais_db'], axis=1)

    # Prepare the data for insertion
    data_to_insert = [(value,) for value in nuevos_paises['nombre_pais'].values.tolist()]

    # Insert the rows into the table
    mysql_hook.insert_rows(table='dimm_pais', rows=data_to_insert, target_fields=['nombre_pais'])
    
    logging.info("Datos cargados de manera exitosa!")


def cargar_dimension_sexo():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    
    query = "SELECT desc_sexo FROM dw.dimm_sexo"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_sexo_db = pd.DataFrame(results, columns=['desc_sexo'])
    else:
        df_dim_sexo_db = pd.DataFrame(columns=['desc_sexo'])

    df_dim_sexo_db = df_dim_sexo_db.rename(columns={'desc_sexo': 'desc_sexo_dim'})
    query = 'SELECT sexo as desc_sexo_staging FROM dw.stage_egresados_niveles'
    results = mysql_hook.get_records(sql=query)
    stage_porcentaje_egresados_internacional = pd.DataFrame(results, columns=['desc_sexo_staging'])

    df_dim_sexo = stage_porcentaje_egresados_internacional[['desc_sexo_staging']].drop_duplicates().reset_index(drop=True)
    nuevos_sexos = pd.merge(df_dim_sexo, df_dim_sexo_db, left_on='desc_sexo_staging', right_on='desc_sexo_dim', how='left')
    nuevos_sexos = nuevos_sexos[nuevos_sexos['desc_sexo_dim'].isnull()].drop(['desc_sexo_dim'], axis=1)

    # Prepare the data for insertion
    data_to_insert = [(value,) for value in nuevos_sexos['desc_sexo_staging'].values.tolist()]

    # Insert the rows into the table
    mysql_hook.insert_rows(table='dimm_sexo', rows=data_to_insert, target_fields=['desc_sexo'])
    
    logging.info("Datos cargados de manera exitosa!")


def cargar_dimm_situacion_laboral():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    query = "SELECT desc_situacion_laboral FROM dw.dimm_situacion_laboral"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_situacion_laboral = pd.DataFrame(results, columns=['desc_situacion_laboral'])
    else:
        df_dim_situacion_laboral = pd.DataFrame(columns=['desc_situacion_laboral'])
    
    df_dim_situacion_laboral_db = df_dim_situacion_laboral.rename(columns={'desc_situacion_laboral': 'desc_situacion_laboral_dim'})

    query = 'SELECT situacion_laboral as desc_situacion_laboral_staging FROM stage_situacion_laboral_egresados'
    results = mysql_hook.get_records(sql=query)
    stage_situacion_laboral_egresados = pd.DataFrame(results, columns=['desc_situacion_laboral_staging'])

    stage_situacion_laboral_egresados = stage_situacion_laboral_egresados[['desc_situacion_laboral_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_situacion_laboral = pd.merge(stage_situacion_laboral_egresados, df_dim_situacion_laboral_db,
                                        left_on='desc_situacion_laboral_staging', right_on='desc_situacion_laboral_dim', how='left')
    
    nuevos_situacion_laboral = nuevos_situacion_laboral[nuevos_situacion_laboral['desc_situacion_laboral_dim'].isnull()].drop(['desc_situacion_laboral_dim'], axis=1)

    # Prepare the data for insertion
    data_to_insert = [(value,) for value in nuevos_situacion_laboral['desc_situacion_laboral_staging'].values.tolist()]

    # Insert the rows into the table
    mysql_hook.insert_rows(table='dimm_situacion_laboral', rows=data_to_insert, target_fields=['desc_situacion_laboral'])
    
    logging.info("Datos cargados de manera exitosa!")


def cargar_dimm_rango_edad():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    query = "SELECT desc_rango_edad FROM dw.dimm_rango_edad"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_rango_edad = pd.DataFrame(results, columns=['desc_rango_edad'])
    else:
        df_dim_rango_edad = pd.DataFrame(columns=['desc_rango_edad'])

    df_dim_rango_edad_db = df_dim_rango_edad.rename(columns={'desc_rango_edad': 'desc_rango_edad_dim'})

    query = "SELECT DISTINCT EDAD AS edad_staging FROM stage_egresados_niveles"
    results = mysql_hook.get_records(sql=query)
    stage_rango_edad = pd.DataFrame(results, columns=['edad_staging'])
    stage_rango_edad = stage_rango_edad[['edad_staging']].drop_duplicates().reset_index(drop=True)

    nuevas_edades = pd.merge(stage_rango_edad, df_dim_rango_edad_db, left_on='edad_staging', right_on='desc_rango_edad_dim', how='left')
    nuevas_edades = nuevas_edades[nuevas_edades['desc_rango_edad_dim'].isnull()].drop(['desc_rango_edad_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevas_edades.iterrows():
            situacion_laboral = {'desc_rango_edad': row['edad_staging']}
            ec_model.append(situacion_laboral)

        # Insert the rows into the table
        mysql_hook.insert_rows(table='dimm_rango_edad', rows=ec_model)

        logging.info("Data loaded successfully!")

    except Exception as e:
        logging.error("Error occurred during data loading: %s", str(e))


def cargar_dimm_tipo_universidad():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    # Read dimension_tipo_universidad
    query = "SELECT desc_tipo_universidad FROM dw.dimm_tipo_universidad"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_tipo_uni_db = pd.DataFrame(results, columns=['desc_tipo_universidad'])
    else:
        df_dim_tipo_uni_db = pd.DataFrame(columns=['desc_tipo_universidad'])

    df_dim_tipo_uni_db = df_dim_tipo_uni_db.rename(columns={'desc_tipo_universidad': 'desc_tipo_universidad_dim'})

    # Read data from staging table
    query = "SELECT tipo_universidad AS desc_tipo_universidad_staging FROM stage_situacion_laboral_egresados"
    results = mysql_hook.get_records(sql=query)
    stage_desc_tipo_universidad = pd.DataFrame(results, columns=['desc_tipo_universidad_staging'])

    df_dim_tipo_uni = stage_desc_tipo_universidad[['desc_tipo_universidad_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_tipos = pd.merge(df_dim_tipo_uni, df_dim_tipo_uni_db,
                            left_on='desc_tipo_universidad_staging', right_on='desc_tipo_universidad_dim', how='left')
    nuevos_tipos = nuevos_tipos[nuevos_tipos['desc_tipo_universidad_dim'].isnull()].drop(
        ['desc_tipo_universidad_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_tipos.iterrows():
            tipo_uni = {'desc_tipo_universidad': row['desc_tipo_universidad_staging']}
            ec_model.append(tipo_uni)

        # Insert the rows into the table
        mysql_hook.insert_rows(table='dimm_tipo_universidad', rows=ec_model)

        logging.info("Data loaded successfully!")
    except Exception as e:
        logging.error("Error occurred during data loading: %s", str(e))



def cargar_dimm_universidades():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    # Read dimension_universidades
    query = "SELECT nombre_universidad FROM dw.dimm_universidades"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_uni_db = pd.DataFrame(results, columns=['nombre_universidad'])
    else:
        df_dim_uni_db = pd.DataFrame(columns=['nombre_universidad'])

    df_dim_uni_db = df_dim_uni_db.rename(columns={'nombre_universidad': 'nombre_universidad_dim'})

    # Read data from staging table
    query = "SELECT nombre_universidad, tipo_universidad, modalidad FROM stage_egresados_universidad"
    results = mysql_hook.get_records(sql=query)
    stage_desc_tipo_universidad = pd.DataFrame(results, columns=['nombre_universidad_staging', 'tipo_universidad', 'modalidad'])

    df_dim_tipo_uni = stage_desc_tipo_universidad[['nombre_universidad_staging', 'tipo_universidad', 'modalidad']].drop_duplicates().reset_index(drop=True)

    nuevos_uni = pd.merge(df_dim_tipo_uni, df_dim_uni_db,
                          left_on='nombre_universidad_staging', right_on='nombre_universidad_dim', how='left')

    nuevos_uni = nuevos_uni[nuevos_uni['nombre_universidad_dim'].isnull()].drop(['nombre_universidad_dim'], axis=1)

    ec_model = []

    try:
        for index, row in nuevos_uni.iterrows():
            universidad = {
                'nombre_universidad': row['nombre_universidad_staging'],
                'tipo_universidad': row['tipo_universidad'],
                'modalidad': row['modalidad']
            }
            ec_model.append(universidad)

        # Insert the rows into the table
        mysql_hook.insert_rows(table='dimm_universidades', rows=ec_model)

        logging.info("Data loaded successfully!")
    except Exception as e:
        logging.error("Error occurred during data loading: %s", str(e))


def cargar_dimm_rama_enseñanza():
    
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    # Read dimension_rama_enseñanza
    query = "SELECT nombre_rama FROM dw.dimm_rama_enseanza"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_rama_db = pd.DataFrame(results, columns=['nombre_rama'])
    else:
        df_dim_rama_db = pd.DataFrame(columns=['nombre_rama'])

    df_dim_rama_db = df_dim_rama_db.rename(columns={'nombre_rama': 'nombre_rama_dim'})

    # Read data from staging table
    query = "SELECT nombre_rama_1 as nombre_rama_staging FROM stage_ramas_conocimiento"
    results = mysql_hook.get_records(sql=query)
    stage_nombre_rama = pd.DataFrame(results, columns=['nombre_rama_staging'])

    df_dim_nombre_rama = stage_nombre_rama[['nombre_rama_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_ramas = pd.merge(df_dim_nombre_rama, df_dim_rama_db,
                            left_on='nombre_rama_staging', right_on='nombre_rama_dim', how='left')

    nuevos_ramas = nuevos_ramas[nuevos_ramas['nombre_rama_dim'].isnull()].drop(
        ['nombre_rama_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_ramas.iterrows():
            rama_con = {
                'id': index + 1, # type: ignore
                'nombre_rama': row['nombre_rama_staging']
            }
            ec_model.append(rama_con)

        # Insert the rows into the table
        mysql_hook.insert_rows(table='dimm_rama_enseanza', rows=ec_model)

        logging.info("Data loaded successfully!")
    except Exception as e:
        logging.error("Error occurred during data loading: %s", str(e))


def cargar_dimm_ambito_enseñanza():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    # Read dimension_ambito_enseñanza
    query = "SELECT desc_ambito FROM dw.dimm_ambito_enseanza"
    results = mysql_hook.get_records(sql=query)

    if results:
        df_dim_rama_db = pd.DataFrame(results, columns=['desc_ambito'])
    else:
        df_dim_rama_db = pd.DataFrame(columns=['desc_ambito'])

    df_dim_rama_db = df_dim_rama_db.rename(columns={'desc_ambito': 'desc_ambito_dim'})

    # Read data from staging table
    query = "SELECT codigo_rama_1, nombre_rama_1, codigo_rama_4, nombre_rama_4 as nombre_ambito_staging FROM stage_ramas_conocimiento"
    results = mysql_hook.get_records(sql=query)
    stage_nombre_rama = pd.DataFrame(results, columns=['codigo_rama_1', 'nombre_rama_1', 'codigo_rama_4', 'nombre_ambito_staging'])

    df_dim_nombre_rama = stage_nombre_rama[['codigo_rama_1', 'nombre_rama_1', 'codigo_rama_4', 'nombre_ambito_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_ambitos = pd.merge(df_dim_nombre_rama, df_dim_rama_db,
                              left_on='nombre_ambito_staging', right_on='desc_ambito_dim', how='left')

    nuevos_ambitos = nuevos_ambitos[nuevos_ambitos['desc_ambito_dim'].isnull()].drop(
        ['desc_ambito_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_ambitos.iterrows():
            rama_con = {
                'id': '0' + str(row['codigo_rama_4']) if len(str(row['codigo_rama_4'])) <= 3 else str(row['codigo_rama_4']),
                'desc_ambito': row['nombre_ambito_staging'],
                'id_rama': row['codigo_rama_1'],
                'nombre_rama': row['nombre_rama_1']
            }
            ec_model.append(rama_con)

        # Insert the rows into the table
        mysql_hook.insert_rows(table='dimm_ambito_enseanza', rows=ec_model)

        logging.info("Data loaded successfully!")
    except Exception as e:
        logging.error("Error occurred during data loading: %s", str(e))