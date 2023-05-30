import pandas as pd
import numpy as np
from airflow.hooks.mysql_hook import MySqlHook
import logging


def load_fact_international_graduated():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    query = 'SELECT * FROM stage_porcentaje_egresados_internacional'
    results = mysql_hook.get_records(sql=query)
    stage_porcentaje_egresados_internacional=pd.DataFrame(results, columns=['year', 'country', 'percentage_graduated', 'percentage_youth_graduated'])
    stage_porcentaje_egresados_internacional['year'] = stage_porcentaje_egresados_internacional['year'].round(0)

    query = 'SELECT * from  stage_numero_egresados_internacional'
    results = mysql_hook.get_records(sql=query)
    stage_numero_egresados_internacional=pd.DataFrame(results, columns=['year', 'country', 'num_graduated_male', 'num_graduated_female', 'num_graduated'])
    stage_numero_egresados_internacional['year'] = stage_numero_egresados_internacional['year'].round(0)

    query = 'SELECT * FROM dimm_pais'
    results = mysql_hook.get_records(sql=query)
    df_dim_pais_db = pd.DataFrame(results, columns=['id', 'nombre_pais'])
    df_dim_pais_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})

    column_order=['year',
    'id_country',
    'num_graduated_male',
    'num_graduated_female',
    'num_graduated',
    'percentage_graduated',
    'percentage_youth_graduated']

    df=pd.merge(stage_numero_egresados_internacional,stage_porcentaje_egresados_internacional,on=['year','country'],how='left')
    fact_international_graduated=pd.merge(df_dim_pais_db,df,on=['country'],how='inner').drop('country',axis=1)
    fact_international_graduated = fact_international_graduated.reindex(columns=column_order)
    fact_international_graduated = fact_international_graduated.fillna('NULL')
    fact_international_graduated = fact_international_graduated.drop_duplicates()

    # Prepare the data for insertion
    data_to_insert = [(value) for value in fact_international_graduated[column_order].values.tolist()]

    # Insert the rows into the table
    mysql_hook.insert_rows(table='fact_international_graduated', rows=data_to_insert, target_fields=column_order)

    logging.info("Datos load_fact_international_graduated cargados exitosamente.")


def load_fact_egresados_rama_ensenanza():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    # Read data from database tables
    df_dim_pais_db = mysql_hook.get_pandas_df('SELECT id as id_pais, nombre_pais AS country FROM dimm_pais')
    df_dim_universidad_db = mysql_hook.get_pandas_df('SELECT id as id_universidad, nombre_universidad FROM dimm_universidades')
    df_dim_rama_enseñanza_db = mysql_hook.get_pandas_df('SELECT id as id_rama_enseanza, nombre_rama FROM dimm_rama_enseanza')

    # Read stage_egresados_universidad table
    stage_egresados_universidad = mysql_hook.get_pandas_df('SELECT ao as year, pais, nombre_universidad, tipo_universidad, modalidad, rama_enseanza, num_egresados FROM stage_egresados_universidad')

    # Merge the dataframes
    df = pd.merge(stage_egresados_universidad, df_dim_pais_db, left_on='pais', right_on='country').drop(['pais', 'country'], axis=1)
    df = pd.merge(df, df_dim_universidad_db, on='nombre_universidad', how='left').drop(['nombre_universidad', 'tipo_universidad', 'modalidad'], axis=1)
    df = pd.merge(df, df_dim_rama_enseñanza_db, left_on='rama_enseanza', right_on='nombre_rama', how='inner').drop(['rama_enseanza', 'nombre_rama'], axis=1)

    column_order = ['year', 'id_pais', 'id_universidad', 'id_rama_enseanza', 'num_egresados']
    df = df.reindex(columns=column_order)

    # Prepare the data for insertion
    data_to_insert = [(value) for value in df[column_order].values.tolist()]

    # Insert the rows into the table
    mysql_hook.insert_rows(table='fact_egresados_rama_enseanza', rows=data_to_insert, target_fields=column_order)

    logging.info("Datos load_fact_egresados_rama_ensenanza cargados exitosamente.")

def load_fact_egresados_niveles():

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    df_pais = mysql_hook.get_pandas_df('SELECT id as id_pais, nombre_pais FROM dimm_pais')
    df_edad = mysql_hook.get_pandas_df('SELECT id as id_rango_edad, desc_rango_edad AS edad FROM dimm_rango_edad')
    df_sexo = mysql_hook.get_pandas_df('SELECT id as id_sexo, desc_sexo AS sexo FROM dimm_sexo')

    df_stage_egresados_niveles = mysql_hook.get_pandas_df('SELECT ao as year, pais, cod_ambito as id_ambito, ambito, sexo, edad, num_egresados_nivel_1, num_egresados_nivel_2 FROM stage_egresados_niveles')
    df_stage_egresados_niveles['num_egresados'] = df_stage_egresados_niveles['num_egresados_nivel_1'] + df_stage_egresados_niveles['num_egresados_nivel_2']
    df_stage_egresados_niveles['nombre_pais'] = 'Spain'

    df_merged = pd.merge(df_stage_egresados_niveles, df_edad, on='edad', how='left')
    df_merged = pd.merge(df_merged, df_sexo, on='sexo', how='left')
    df_merged = pd.merge(df_merged, df_pais, on='nombre_pais', how='left')
    df_merged = df_merged.drop_duplicates()

    column_order = ['year', 'id_pais', 'id_ambito', 'id_sexo', 'id_rango_edad', 'num_egresados_nivel_1', 'num_egresados_nivel_2', 'num_egresados']
    df_merged = df_merged.reindex(columns=column_order)
    df_merged = df_merged.fillna(0)
    df_merged = df_merged.replace('nan', 0)

    # Prepare the data for insertion
    data_to_insert = [(value) for value in df_merged[column_order].values.tolist()]

    # Insert the rows into the table
    mysql_hook.insert_rows(table='fact_egresados_niveles', rows=data_to_insert, target_fields=column_order)

    logging.info("Datos fact_egresados_niveles cargados exitosamente.")

def load_fact_situacion_laboral_egresados():
    
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    df_pais = mysql_hook.get_pandas_df('SELECT id as id_pais, nombre_pais AS pais FROM dimm_pais')
    df_univer = mysql_hook.get_pandas_df('SELECT id as id_tipo_universidad, desc_tipo_universidad AS tipo_universidad FROM dimm_tipo_universidad')
    df_sexo = mysql_hook.get_pandas_df('SELECT id as id_sexo, LOWER(desc_sexo) AS sexo FROM dimm_sexo')
    df_rama = mysql_hook.get_pandas_df('SELECT id as id_ambito, LOWER(nombre_rama) AS area_estudio FROM dimm_ambito_enseanza')
    df_situa = mysql_hook.get_pandas_df('SELECT id as id_situacion_laboral, desc_situacion_laboral AS situacion_laboral FROM dimm_situacion_laboral')

    df_stage_egresados_niveles = mysql_hook.get_pandas_df('SELECT ao as year, pais, tipo_universidad, area_estudio, sexo, situacion_laboral, cantidad FROM stage_situacion_laboral_egresados')
    df_stage_egresados_niveles['area_estudio'] = df_stage_egresados_niveles['area_estudio'].str.lower().str.strip()
    df_stage_egresados_niveles['pais'] = 'Spain'

    df_merged = pd.merge(df_stage_egresados_niveles, df_sexo, on='sexo', how='left')
    df_merged = pd.merge(df_merged, df_pais, on='pais', how='left')
    df_merged = pd.merge(df_merged, df_univer, on='tipo_universidad', how='left')
    df_merged = pd.merge(df_merged, df_rama, on='area_estudio', how='left')
    df_merged = pd.merge(df_merged, df_situa, on='situacion_laboral', how='left')

    df_merged.rename(columns={'id_ambito': 'id_area_estudio'}, inplace=True)

    df_merged = df_merged[['year', 'id_pais', 'id_tipo_universidad', 'id_area_estudio', 'id_sexo', 'id_situacion_laboral', 'cantidad']]
    df_merged = df_merged.fillna('NULL')
    df_merged.drop_duplicates(keep='first', inplace=True)

    data_to_insert = [tuple(row) for row in df_merged.values]

    logging.info(data_to_insert)

    mysql_hook.insert_rows(table='fact_situacion_laboral_egresados', rows=data_to_insert, target_fields=df_merged.columns.tolist())
    
    logging.info("Datos fact_situacion_laboral_egresados cargados exitosamente.")
