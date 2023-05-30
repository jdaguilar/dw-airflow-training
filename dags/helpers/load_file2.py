from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd


def preprocesar_archivo_egresados_niveles(file: str,directory: str):

    #Preprocesar archivos egresados niveles grad_5sc.csv

    df=pd.read_csv(directory + file, encoding='latin-1', delimiter=';')
    cod_ambito_ambito=df['COD_AMBITO'].str.split("-",n=1,expand=True)
    df['COD_AMBITO']=cod_ambito_ambito[0]
    df['AMBITO']=cod_ambito_ambito[1]
    df['PAIS']='ESPAÑA'
    df['ANIO']= '2017'
    
    df = df.reindex(columns=['ANIO','PAIS','COD_AMBITO','AMBITO','SEXO','EDAD','NUM_EGR_NV1','NUM_EGR_NV2'])
    df=df.reset_index(drop=True)

    df.to_csv("/tmp/data/processed/egresados_niveles.csv", index=False)
    print("Archivo generado exitosamente.")


def cargar_archivo_egresados_niveles():

    df = pd.read_csv("/tmp/data/processed/egresados_niveles.csv", encoding ='latin-1', delimiter=',')

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    mysql_hook.insert_rows(table='stage_egresados_niveles', rows=df.values.tolist())

    print("Datos insertados exitosamente.")
