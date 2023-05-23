from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

def preprocesar_archivo_ramas_conocimiento(file: str,directory: str):

    #Preprocesar archivo stage_ramas_conocimiento

    df = pd.read_csv(directory + file, encoding='latin-1', delimiter=';')

    columns = df.columns
    new_columns_dict = {}
    cod_count = 1
    nom_count = 1

    for col in columns:
        if 'COD_' in col:
            new_columns_dict[col] = f'codigo_rama_{cod_count}'
            cod_count += 1
        elif 'NOM_' in col:
            new_columns_dict[col] = f'nombre_rama_{nom_count}'
            nom_count += 1
        else:
            new_columns_dict[col] = col

    # Use the rename() method to rename the columns
    df.rename(columns=new_columns_dict, inplace=True)

    df_final = df
 
    df_final.to_csv("/tmp/data/processed/ramas_conocimiento.csv", index=False)
    print("Archivo generado exitosamente.")


def cargar_archivo_ramas_conocimiento():

    df = pd.read_csv("/tmp/data/processed/ramas_conocimiento.csv", encoding ='latin-1', delimiter=',')

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    mysql_hook.insert_rows(table='stage_ramas_conocimiento', rows=df.values.tolist())

    print("Datos insertados exitosamente.")
