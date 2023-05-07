import utils.connect_db as connect_db
import processed.stage_egresados_niveles as fn_stage_egresados_niveles
import processed.stage_porcentaje_egresados_internacional as fn_stage_porcentaje_egresados_internacional
import processed.stage_situacion_laboral_egresados as fn_stage_situacion_laboral_egresados
import pandas as pd
import numpy as np
from utils.models import *

def dimension_pais():
    sqlEngine, dbConnection,session=connect_db.db_connector()
    #read dimension_pais
    query = session.query(Pais.nombre_pais)
    df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_pais_db=df_dim_pais_db.rename(columns={'nombre_pais':'nombre_pais_db'})
    
    #read stage_porcentaje_egresados_internacional
    query='SELECT distinct GEO_TIME FROM stage_porcentaje_egresados_internacional'
    stage_porcentaje_egresados_internacional=connect_db.get_table_db_staging(session,query)
    
    df_dim_pais=stage_porcentaje_egresados_internacional[['GEO_TIME']].drop_duplicates().reset_index(drop=True)
    df_dim_pais=df_dim_pais.rename(columns={'GEO_TIME':'nombre_pais'})

    nuevos_paises=pd.merge(df_dim_pais,df_dim_pais_db,left_on='nombre_pais',right_on='nombre_pais_db',how='left')
    nuevos_paises=nuevos_paises[nuevos_paises['nombre_pais_db'].isnull()].drop(['nombre_pais_db'],axis=1)
    print(len(nuevos_paises))
    #pensarlo mejor
    ec_model=[]
    try:
        for index,row in nuevos_paises.iterrows():
            print(row)
            pais=(Pais(
                nombre_pais=row['nombre_pais']))
            ec_model.append(pais)
            print("pasa por aca")

                # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
            # Confirmar la transacción
        session.commit()
        print('se guarda los datos')
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()

def dimension_sexo():
    sqlEngine, dbConnection,session=connect_db.db_connector()
    #read dimension_pais
    query = session.query(Sexo.desc_sexo)
    df_dim_sexo_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_sexo_db=df_dim_sexo_db.rename(columns={'desc_sexo':'desc_sexo_dim'})

    
    #read stage_porcentaje_egresados_internacional
    query='SELECT sexo as desc_sexo_staging FROM stage_egresados_niveles'
    stage_porcentaje_egresados_internacional=connect_db.get_table_db_staging(session,query)
    
    
    df_dim_sexo=stage_porcentaje_egresados_internacional[['desc_sexo_staging']].drop_duplicates().reset_index(drop=True)
   

    nuevos_sexos=pd.merge(df_dim_sexo,df_dim_sexo_db,left_on='desc_sexo_staging',right_on='desc_sexo_dim',how='left')
    nuevos_sexos=nuevos_sexos[nuevos_sexos['desc_sexo_dim'].isnull()].drop(['desc_sexo_dim'],axis=1)
    
    ec_model=[]
    try:
        for index,row in nuevos_sexos.iterrows():

   
            sexo=Sexo(
                desc_sexo=row['desc_sexo_staging'])
            ec_model.append(sexo)
        print("pasa por aca")

            # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()

def dimm_situacion_laboral():
    sqlEngine, dbConnection,session=connect_db.db_connector()
    #read dimension_situacion_laboral
    query = session.query(Situacion_laboral.desc_situacion_laboral)
    df_dim_situacion_laboral= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_situacion_laboral_db=df_dim_situacion_laboral.rename(columns={'desc_situacion_laboral':'desc_situacion_laboral_dim'})

    
    #read stage_porcentaje_egresados_internacional
    query='SELECT situacion_laboral as desc_situacion_laboral_staging FROM stage_situacion_laboral_egresados'
    stage_situacion_laboral_egresados=connect_db.get_table_db_staging(session,query)
    
    
    stage_situacion_laboral_egresados=stage_situacion_laboral_egresados[['desc_situacion_laboral_staging']].drop_duplicates().reset_index(drop=True)
   

    nuevos_situacion_laboral=pd.merge(stage_situacion_laboral_egresados,df_dim_situacion_laboral_db,left_on='desc_situacion_laboral_staging',right_on='desc_situacion_laboral_dim',how='left')
    nuevos_situacion_laboral=nuevos_situacion_laboral[nuevos_situacion_laboral['desc_situacion_laboral_dim'].isnull()].drop(['desc_situacion_laboral_dim'],axis=1)
    print(nuevos_situacion_laboral)
    ec_model=[]
    try:
        for index,row in nuevos_situacion_laboral.iterrows():

   
            situacion_laboral=Situacion_laboral(
                desc_situacion_laboral=row['desc_situacion_laboral_staging'])
            ec_model.append(situacion_laboral)
        print("pasa por aca")

            # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()

def dimm_rango_edad():
    sqlEngine, dbConnection,session=connect_db.db_connector()
    #read dimension_rango_edad
    query = session.query(Rango_edad.desc_rango_edad)
    df_dim_rango_edad= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_rango_edad_db=df_dim_rango_edad.rename(columns={'desc_rango_edad':'desc_rango_edad_dim'})

    
    #read stage_porcentaje_egresados_internacional
    query='SELECT distinct EDAD as edad_staging FROM stage_egresados_niveles'
    stage_rango_edad=connect_db.get_table_db_staging(session,query)
    
    
    stage_rango_edad=stage_rango_edad[['edad_staging']].drop_duplicates().reset_index(drop=True)
   

    nuevas_edades=pd.merge(stage_rango_edad,df_dim_rango_edad_db,left_on='edad_staging',right_on='desc_rango_edad_dim',how='left')
    nuevas_edades=nuevas_edades[nuevas_edades['desc_rango_edad_dim'].isnull()].drop(['desc_rango_edad_dim'],axis=1)
    print(nuevas_edades)
    ec_model=[]
    try:
        for index,row in nuevas_edades.iterrows():

   
            situacion_laboral=Rango_edad(
                desc_rango_edad=row['edad_staging'])
            ec_model.append(situacion_laboral)
        print("pasa por aca")

            # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def main():
    dimension_pais()
    dimension_sexo()
    dimm_situacion_laboral()
    dimm_rango_edad()


main()