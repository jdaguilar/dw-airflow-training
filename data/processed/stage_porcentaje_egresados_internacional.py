
import pandas as pd
import mysql.connector
import connect_db
#1

df=pd.read_excel(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\educ_uoe_grad05.xlsx")
df2=pd.read_excel(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\educ_uoe_grad05.xlsx",sheet_name='Data2')

df['flag']=1
df2['flag']=2


def transformation(df):
    flag=df['flag'][0]
    #escoger la data que necesito
    df=df.iloc[9:,:6].reset_index(drop=True)

    #coger la fila que quiero como columna
    new_header=df.iloc[0]#.apply(pd.to_numeric, errors='coerce')
    #eliminar esa fila
    df = df[1:]
    #poner esa fila como header
    df.columns=new_header

    names_columns=['GEO_TIME','2013','2014','2015','2016', '2017']
    df.columns=names_columns

    df=df[df['GEO_TIME']!=":"]
    df=df[df['GEO_TIME']!="Special value:"]
    # eliminar registros con valores NaN en la columna "x"
    df = df.dropna(subset=['GEO_TIME'])
    # reemplazar ":" por 0 en todo el DataFrame
    df = df.replace(':', 0, regex=True)
    if flag==1:
        value_name='percentage_graduated'
    else:
        value_name='percentage_youth_graduated'
    #derretir el DataFrame en una estructura más larga
    df = pd.melt(df, id_vars=['GEO_TIME'], value_vars=['2013', '2014', '2015', '2016', '2017'], 
                 var_name='year', value_name=value_name)

    # agregar una columna para el porcentaje de jóvenes graduados (asumiendo que es el mismo que el porcentaje general)
    #df['percentage_youth_graduated'] = df['percentage_graduated']

    # pivotear el DataFrame para obtener la estructura deseada
    df = pd.pivot_table(df, values=[value_name], index=['year', 'GEO_TIME'])

    df = df.reset_index()
    return df


def merge_df(df,df2):
    df_final=df.merge(df2,on=["year","GEO_TIME"])
    return df_final

if __name__ == "__main__":
    df=transformation(df)
    df2=transformation(df2)
    df=merge_df(df,df2)


    sqlEngine, dbConnection=connect_db.db_connector()
    name_table='stage_porcentaje_egresados_internacional'
    connect_db.create_table(df,name_table,dbConnection)
    print("finaliza")

