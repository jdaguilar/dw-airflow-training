import pandas as pd
import connect_db
#2
df=pd.read_csv(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\grad_5sc.csv", encoding='ISO-8859-1', delimiter=';')

def transformation(df):
    cod_ambito_ambito=df['COD_AMBITO'].str.split("-",n=1,expand=True)
    df['COD_AMBITO']=cod_ambito_ambito[0]
    df['AMBITO']=cod_ambito_ambito[1]
    df['PAIS']='ESPAÑA'
    df['ANIO']= '2016-2017'
    df = df.reindex(columns=['COD_AMBITO','AMBITO','SEXO','EDAD','NUM_EGR_NV1','NUM_EGR_NV2','ANIO'])
    return df



df=transformation(df)
sqlEngine, dbConnection=connect_db.db_connector()
name_table="stage_egresados_niveles"
connect_db.create_table(df,name_table,dbConnection)
