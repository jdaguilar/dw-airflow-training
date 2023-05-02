import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text

def db_connector():

    """CONEXION A LA BASE DE DATOS
    Returns:
        sqlEngine (sqlalchemy.engine.base.Engine) : Sirve para utilizarlo como conexion y asi, guardar informacion a la base de datos
        dbConnection (sqlalchemy.engine.base.Connection) :Sirve para utilizarlo como conexion y asi, leer informacion de la base datos
    """
    try:
        cadena_conexion = "mysql+pymysql://root:1234@localhost:3306/pragma"
        sqlEngine = create_engine(cadena_conexion)
        dbConnection = sqlEngine.connect()
        print(dbConnection)
    except BaseException as e :
        print(e)
        sqlEngine=None
        dbConnection=None
        print("No se conectò")
    return sqlEngine, dbConnection

def create_table(df,name_table,dbConnection):
    df.to_sql(name=name_table, con=dbConnection, if_exists='replace')
