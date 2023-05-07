import pandas as pd
#import handle_transformation
#from handle_transformation import execution_date
#from data.utils import handle_transformation
from datetime import datetime
import pytz

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
    cod_ambito_ambito=df['COD_AMBITO'].str.split("-",n=1,expand=True)
    df['COD_AMBITO']=cod_ambito_ambito[0]
    df['AMBITO']=cod_ambito_ambito[1]
    df['PAIS']='ESPAÑA'
    df['ANIO']= '2016-2017'
    df = df.reindex(columns=['COD_AMBITO','AMBITO','SEXO','EDAD','NUM_EGR_NV1','NUM_EGR_NV2','ANIO'])
    df=df.reset_index(drop=True)
    df=execution_date(df)
    return df
