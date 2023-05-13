import utils.connect_db as connect_db
import processed.stage_egresados_niveles as fn_stage_egresados_niveles
import processed.stage_porcentaje_egresados_internacional as fn_stage_porcentaje_egresados_internacional
import processed.stage_situacion_laboral_egresados as fn_stage_situacion_laboral_egresados
import pandas as pd
from utils.models import Pais,Fact_international_graduated

def fact_international_graduated():

    sqlEngine, _,session=connect_db.db_connector()
    #read stage_porcentaje_egresados_internacional
    query='SELECT * FROM stage_porcentaje_egresados_internacional'
    stage_porcentaje_egresados_internacional=connect_db.get_table_db_staging(session,query)

    query='SELECT * from  stage_numero_egresados_internacional'
    stage_numero_egresados_internacional=connect_db.get_table_db_staging(session,query)

    query = session.query(Pais)
    df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_pais_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})

    column_order=['year',
    'id_country',
    'num_graduated_M',
    'num_graduated_F',
    'num_graduated',
    'percentage_graduated',
    'percentage_youth_graduated',
    'fecha_ejecucion']

    df=pd.merge(stage_numero_egresados_internacional,stage_porcentaje_egresados_internacional,on=['year','country'],how='left')
    fact_international_graduated=pd.merge(df_dim_pais_db,df,on=['country'],how='inner').drop('country',axis=1)
    fact_international_graduated = fact_international_graduated.reindex(columns=column_order)

    print(fact_international_graduated)

        #pensarlo mejor
    ec_model=[]
    try:
        for index,row in fact_international_graduated.iterrows():
            international_graduated=(Fact_international_graduated(
                year=row['year'],
                id_country=row['id_country'],
                num_graduated_male=row['num_graduated_M'],
                num_graduated_female=row['num_graduated_F'],
                num_graduated=row['num_graduated'],
                percentage_graduated=row['percentage_graduated'],
                percentage_youth_graduated=row['percentage_youth_graduated'])
                )
            ec_model.append(international_graduated)

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


def fact_egresados_rama_enseñanza():
    # fact_egresados_rama_enseñanza
    sqlEngine, dbConnection,session=connect_db.db_connector()
    query = session.query(Pais)
    df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_pais_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})


    #esperar q haya dimension
    # query = session.query(universidad.id, universidad.nombre.universidad)
    # df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    # df_dim_universidad_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})

    #esperar q haya dimension
    # query = session.query(rama_enseñanza.id, rama_enseñanza.nombre.nombre_rama)
    # df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    # df_dim_rama_enseñanza_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})


    sqlEngine, dbConnection,session=connect_db.db_connector()
    #read stage_porcentaje_egresados_internacional
    query='SELECT * FROM stage_egresados_universidad'
    stage_egresados_universidad=connect_db.get_table_db_staging(session,query)

    #descomentar ya cuando tenga todas las dimensiones 


    df=pd.merge(stage_egresados_universidad,df_dim_pais_db,left_on='pais',right_on='country').drop(['pais','country','index'],axis=1)

    # df=pd.merge(df,df_dim_universidad_db, on=['nombre_universidad','tipo_universidad'],how='left').drop(['nombre_universidad','tipo_universidad','modalidad'],axis=1)
    # df=pd.merge(df,df_dim_rama_enseñanza_db, left_on=['rama_enseñanza'],rigth_on=['nombre_rama'],how='left').drop(['rama_enseñanza'])

    #column_order=['año','id_pais','id_universidad','id_rama_enseñanza','num_egresados']
    #df = df.reindex(columns=column_order)


def main():
    fact_international_graduated()