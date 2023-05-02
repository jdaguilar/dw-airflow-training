import pandas as pd
import mysql.connector
import connect_db

def transformation(df):
    df=df.iloc[8:,0:5].reset_index(drop=True)
    df.columns = ["area_estudio","cantidad_total","trabajando","desempleo","inactivo"]

    dfs = []
    start = 0
    for i in range(len(df)):
        if pd.isnull(df.loc[i, 'trabajando']):
            dfs.append(df.iloc[start:i,:])
            start = i + 1
    dfs.append(df.iloc[start:,:])

    df_lista=[]
    # Iterar a través de la lista
    for i in range(len(dfs)):
        # Comprobar la longitud del dataframe
        if len(dfs[i])> 0:
            # Eliminar el dataframe de la lista
            df_lista.append(dfs[i])



    #para el primer df
    column_general=["ambos_sexos","hombres","mujeres"]
    tipo_universidad=["total","publico","privadas"]

    for i in range(len(df_lista)):
        if i<=2:
            df_lista[i]['sexo'] = column_general[0]
        if i>=3 and i<=5:
            df_lista[i]['sexo'] = column_general[1]

        if i>=6 and i<=8:
            df_lista[i]['sexo'] = column_general[2]

        tipo = tipo_universidad[i % len(tipo_universidad)]
        df_lista[i]['tipo_universidad'] = tipo
        df_lista[i]['anio'] = 2014
        df_lista[i]['pais'] = 'chile'



    #unir los dfs

    df_concatenado = pd.concat(df_lista)

    df = df_concatenado[(df_concatenado['tipo_universidad'] != 'total') & (df_concatenado['area_estudio'].str.strip() != 'Total')]



    return df

def pivote(df_concatenado):
    # Realizamos el pivoteo
    df = pd.melt(df_concatenado, id_vars=['area_estudio', 'cantidad_total', 'sexo', 'tipo_universidad', 'anio', 'pais'],
                value_vars=['trabajando', 'desempleo', 'inactivo'],
                var_name='situacion_laboral', value_name='cantidad')

    # Ordenamos el DataFrame según nuestras especificaciones
    df = df[['anio', 'pais', 'tipo_universidad', 'area_estudio', 'sexo', 'situacion_laboral', 'cantidad']]




    return df


if __name__ == "__main__":
    df=pd.read_excel(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\03003.xlsx")
    df=transformation(df)
    df=pivote(df)

    sqlEngine, dbConnection=connect_db.db_connector()
    print(sqlEngine, dbConnection)
    name_table='stage_situacion_laboral_egresados'
    connect_db.create_table(df,name_table,dbConnection)
    print("se hizo")