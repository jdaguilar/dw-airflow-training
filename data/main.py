import utils.connect_db as connect_db
import processed.stage_egresados_niveles as fn_stage_egresados_niveles
import processed.stage_porcentaje_egresados_internacional as fn_stage_porcentaje_egresados_internacional
import processed.stage_situacion_laboral_egresados as fn_stage_situacion_laboral_egresados
import pandas as pd
from utils.models import Pais


def main_staging():
    #stage_egresados_niveles
    '''
    stage_egresados_niveles
    '''
    stage_egresados_niveles=pd.read_csv(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\grad_5sc.csv", encoding='ISO-8859-1', delimiter=';')
    stage_egresados_niveles=fn_stage_egresados_niveles.transformation(stage_egresados_niveles)
    _, dbConnection,_=connect_db.db_connector()
    name_table="stage_egresados_niveles"
    connect_db.create_table(stage_egresados_niveles,name_table,dbConnection)

    
    '''
    stage_porcentaje_egresados_internacional
    '''
    stage_porcentaje_egresados_internacional_1=pd.read_excel(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\educ_uoe_grad05.xlsx")
    stage_porcentaje_egresados_internacional_2=pd.read_excel(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\educ_uoe_grad05.xlsx",sheet_name='Data2')
    stage_porcentaje_egresados_internacional_1['flag']=1
    stage_porcentaje_egresados_internacional_2['flag']=2

    stage_porcentaje_egresados_internacional_1=fn_stage_porcentaje_egresados_internacional.transformation(stage_porcentaje_egresados_internacional_1)
    stage_porcentaje_egresados_internacional_2=fn_stage_porcentaje_egresados_internacional.transformation(stage_porcentaje_egresados_internacional_2)
    stage_porcentaje_egresados_internacional=fn_stage_porcentaje_egresados_internacional.merge_df(stage_porcentaje_egresados_internacional_1,stage_porcentaje_egresados_internacional_2)
    _, dbConnection,_=connect_db.db_connector()
    name_table='stage_porcentaje_egresados_internacional'
    connect_db.create_table(stage_porcentaje_egresados_internacional,name_table,dbConnection)
    print("finaliza")

    '''
    stage_situacion_laboral_egresados
    '''

    stage_situacion_laboral_egresados=pd.read_excel(r"C:\Users\marcelo.diezm_pragma\Documents\reto_airflow\dw-airflow-training\data\raw\03003.xlsx")
    stage_situacion_laboral_egresados=fn_stage_situacion_laboral_egresados.transformation(stage_situacion_laboral_egresados)
    stage_situacion_laboral_egresados=fn_stage_situacion_laboral_egresados.pivote(stage_situacion_laboral_egresados)
    _, dbConnection,_=connect_db.db_connector()
    name_table='stage_situacion_laboral_egresados'
    connect_db.create_table(stage_situacion_laboral_egresados,name_table,dbConnection)
    print("se hizo")
    print(stage_porcentaje_egresados_internacional.columns)





dimensions()