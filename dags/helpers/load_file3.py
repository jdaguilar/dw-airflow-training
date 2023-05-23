from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

def preprocesar_archivo_egresados_internacional(file: str,directory: str):

    #Preprocesar archivos egresados internacional educ_uoe_grad05
    df1=pd.read_excel(directory + file)
    df2=pd.read_excel(directory + file,sheet_name='Data2')

    df1['flag']=1
    df2['flag']=2
    
    df_list = [df1, df2]
    df_transformed = []

    for df in df_list:

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
        df = pd.melt(df, id_vars=['GEO_TIME'], value_vars=['2013', '2014', '2015', '2016', '2017'], var_name='year', value_name=value_name)

        # pivotear el DataFrame para obtener la estructura deseada
        df = pd.pivot_table(df, values=[value_name], index=['year', 'GEO_TIME'])

        df = df.reset_index()
        df.rename(columns={'GEO_TIME':'country'},inplace=True)

        df_transformed.append(df)
    

    df_transformed_1 = df_transformed[0]
    df_transformed_2 = df_transformed[1]
    
    df_final=df_transformed_1.merge(df_transformed_2,on=["year","country"])

    df_final.to_csv("/tmp/data/processed/egresados_internacional.csv", index=False)
    print("Archivo generado exitosamente.")


def cargar_archivo_egresados_internacional():

    df = pd.read_csv("/tmp/data/processed/egresados_internacional.csv", encoding ='latin-1', delimiter=',')

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')

    mysql_hook.insert_rows(table='stage_porcentaje_egresados_internacional', rows=df.values.tolist())

    print("Datos insertados exitosamente.")
