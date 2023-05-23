

from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

def preprocesar_archivo_egresados_universidad(file1: str, file2: str,directory: str):

    #Preprocesar archivos egresados universidad SEGR1.csv y SEGR2.csv

    df_1 = pd.read_csv(directory + file1, encoding ='latin-1', delimiter=';')
    df_2 = pd.read_csv(directory + file2, encoding ='latin-1', delimiter=';')

    df_union = pd.concat([df_1, df_2])

    data_cols = df_union.columns

    id_columns = [col for col in data_cols if not 'EGR_' in col]
    data_columns = [col for col in data_cols if 'EGR_' in col]

    #Consolidar información de todos los años en una sola columna
    df_unpivot = pd.melt(df_union, id_vars=id_columns, value_vars=data_columns, var_name='año', value_name='num_egresados')

    #Actualizar columna de año usando los últimos dos digitos del encabezado
    df_unpivot['año'] = '20' + df_unpivot['año'].str[-2:]

    df_unpivot['pais'] = 'Spain'

    df_unpivot.rename(columns={'UNIVERSIDAD':'nombre_universidad', 'TIPO_UNIVERSIDAD': 'tipo_universidad', 'MODALIDAD': 'modalidad', 'RAMA_ENSEÑANZA':'rama_enseñanza'}, inplace=True)

    df_unpivot = df_unpivot[['año','pais','nombre_universidad','tipo_universidad','modalidad','rama_enseñanza','num_egresados']]

    df_unpivot.to_csv("/tmp/data/processed/egresados_universidad.csv", index=False)

    print("Archivo generado exitosamente.")


def cargar_archivo_egresados_universidad():

    df = pd.read_csv("/tmp/data/processed/egresados_universidad.csv", encoding ='latin-1', delimiter=',')

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    mysql_hook.insert_rows(table='stage_egresados_universidad', rows=df.values.tolist())

    print("Datos insertados exitosamente.")


