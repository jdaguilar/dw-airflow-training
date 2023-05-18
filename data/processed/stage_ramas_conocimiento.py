
import pandas as pd
from datetime import datetime
import pytz

default_df = pd.DataFrame()

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

    df = df.reset_index()
    return df


def merge_df(df=default_df, df2=default_df):

    df_final=execution_date(df)
    return df_final
