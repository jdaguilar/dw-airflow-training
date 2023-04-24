from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

def dwh_connector(rds_secret="rds_datospragma_pdn",local=False):
    """Makes the conection to the DWH

    Returns:
        object: Session engine to RDS PosgeSQL Aurora
    """
    str_connection = f"postgresql+psycopg2://{rds_datospragma_pdn['db_username']}:" \
    f"{rds_datospragma_pdn['db_password']}@{rds_datospragma_pdn['rds_host']}:" \
    f"{rds_datospragma_pdn['rds_port']}/staging_area"

    return  engine,Session