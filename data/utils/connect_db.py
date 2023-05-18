from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
def db_connector():

    """CONEXION A LA BASE DE DATOS
    Returns:
        sqlEngine (sqlalchemy.engine.base.Engine) : Sirve para utilizarlo como conexion y asi, guardar informacion a la base de datos
        dbConnection (sqlalchemy.engine.base.Connection) :Sirve para utilizarlo como conexion y asi, leer informacion de la base datos
    """
    try:
        #cadena_conexion = "mysql+pymysql://root:1234@localhost:3306/pragma"
        cadena_conexion = "mysql://admin:admin@172.18.0.8:3306/dw"
        auth_plugin='mysql_native_password'
        sqlEngine = create_engine(cadena_conexion)
        dbConnection = sqlEngine.connect()
        Session = sessionmaker(bind=sqlEngine)
        session = Session()

    except BaseException as e :
        print(e)
        sqlEngine=None
        dbConnection=None
        session=None
        print("No se conecto")
    return sqlEngine, dbConnection,session

def create_table(df,name_table,dbConnection):
    df.to_sql(name=name_table, con=dbConnection, if_exists='replace',index=False)
    dbConnection.close()


# Hacemos una consulta de ejemplo a la base de datos

def get_table_db_staging(session,query):
    resultados = session.execute(query)
    filas = []
    # Imprimimos los resultados
    for resultado in resultados:
        filas.append(resultado)
    df = pd.DataFrame(filas)  
    # Cerramos la sesión
    session.close()
    return df

