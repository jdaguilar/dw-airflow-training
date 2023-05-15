
import pandas as pd
import mysql.connector
import data.utils.connect_db as connect_db
from datetime import datetime
import pytz

#1

def execution_date(df) -> pd.DataFrame:
        """
        Adds a new column to a pandas DataFrame with the current execution date.

        Returns
        -------
        pd.DataFrame
            The modified DataFrame with a new column called "fecha_ejecucion" containing the execution date.
        """
        tz=pytz.timezone("America/Bogota")
        fecha_actual = datetime.now(tz)
        fecha_formateada = fecha_actual.strftime("%Y-%m-%d")
        fecha_formateada=int(fecha_formateada.replace("-",""))
        df['fecha_ejecucion'] = fecha_formateada

        return df

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
    df.rename(columns={'GEO_TIME':'country'},inplace=True)
    return df


def merge_df(df,df2):
    df_final=df.merge(df2,on=["year","country"])
    df_final=execution_date(df_final)
    return df_final


