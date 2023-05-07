
from sqlalchemy import Column, String, Integer, DateTime, Float, Date, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Date,Boolean
Base = declarative_base()
class Pais(Base):
    """Class for model declaration ENPS Questions
    """
    __tablename__ = 'dimm_pais'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    nombre_pais = Column(String, nullable=False)



    def __init__(self, nombre_pais) -> None:
        self.nombre_pais = nombre_pais


class Sexo(Base):
    """Class for model declaration ENPS Questions
    """
    __tablename__ = 'dimm_sexo'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    desc_sexo = Column(String, nullable=False)



    def __init__(self,desc_sexo) -> None:
        self.desc_sexo = desc_sexo

class Situacion_laboral(Base):
    """Class for model declaration ENPS Questions
    """
    __tablename__ = 'dimm_situacion_laboral'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    desc_situacion_laboral = Column(String, nullable=False)

    def __init__(self,desc_situacion_laboral) -> None:
        self.desc_situacion_laboral = desc_situacion_laboral


class Rango_edad(Base):
    """Class for model declaration ENPS Questions
    """
    __tablename__ = 'dimm_rango_edad'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    desc_rango_edad = Column(String, nullable=False)

    def __init__(self,desc_rango_edad) -> None:
        self.desc_rango_edad = desc_rango_edad


