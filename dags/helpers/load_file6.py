from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

def preprocesar_archivo_num_egresados_inter(file: str,directory: str):

    #Preprocesar archivo num_egresados_inter educ_uoe_grad01.xlsx

    df_1 = pd.read_excel(directory + file, sheet_name='Data', skiprows=11, nrows=13)
    df_2 = pd.read_excel(directory + file, sheet_name='Data2', skiprows=11, nrows=13)
    df_3 = pd.read_excel(directory + file, sheet_name='Data3', skiprows=11, nrows=13)

    df_1.drop('Unnamed: 6', inplace=True, axis=1)

    #Consolidar información de todos los años en una sola columna
    df1_unpivot = pd.melt(df_1, id_vars=['GEO/TIME'], value_vars=['2013','2014','2015','2016','2017'], var_name='year', value_name='num_graduated')

    #Consolidar información de todos los años en una sola columna
    df2_unpivot = pd.melt(df_2, id_vars=['GEO/TIME'], value_vars=['2013','2014','2015','2016','2017'], var_name='year', value_name='num_graduated_M')

    #Consolidar información de todos los años en una sola columna
    df3_unpivot = pd.melt(df_3, id_vars=['GEO/TIME'], value_vars=['2013','2014','2015','2016','2017'], var_name='year', value_name='num_graduated_F')

    df1_unpivot.rename(columns={'GEO/TIME': 'country'}, inplace=True)
    df2_unpivot.rename(columns={'GEO/TIME': 'country'}, inplace=True)
    df3_unpivot.rename(columns={'GEO/TIME': 'country'}, inplace=True)

    df1_unpivot.replace(':', 0, inplace=True)
    df2_unpivot.replace(':', 0, inplace=True)
    df3_unpivot.replace(':', 0, inplace=True)

    merge_df = df1_unpivot.merge(df2_unpivot, on=['country','year'] , how='left')
    merge_df = merge_df.merge(df3_unpivot, on=['country','year'], how='left')
    
    #Cambiar el orden de las columnas
    df_final = merge_df[['year','country','num_graduated_M','num_graduated_F','num_graduated']]

    df_final = df_final[df_final['country'].notnull()]

    df_final.to_csv("/tmp/data/processed/numero_egresados_inter.csv", index=False)
    print("Archivo generado exitosamente.")


def cargar_archivo_num_egresados_inter():

    df = pd.read_csv("/tmp/data/processed/numero_egresados_inter.csv", encoding ='latin-1', delimiter=',')

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    mysql_hook.insert_rows(table='stage_numero_egresados_internacional', rows=df.values.tolist())

    print("Datos insertados exitosamente.")
