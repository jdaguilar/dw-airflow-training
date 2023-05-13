
from sqlalchemy import Column, String, Integer, DateTime, Float, Date, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Date,Boolean
Base = declarative_base()

#dimensiones

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


#facts

class Fact_international_graduated(Base):
    """Class for model declaration ENPS Questions
    """
    __tablename__ = 'fact_international_graduated'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    year = Column(Integer, primary_key=True)
    id_country = Column(Integer, nullable=False)
    num_graduated_male = Column(Integer, primary_key=True)
    num_graduated_female = Column(Integer, primary_key=True)
    num_graduated = Column(Integer, primary_key=True)
    percentage_graduated = Column(Float, primary_key=True)
    percentage_youth_graduated = Column(Float, primary_key=True)
    year = Column(Integer, primary_key=True)

    def __init__(self,
                 year,
                 id_country,
                 num_graduated_male,
                 num_graduated_female,
                 num_graduated,
                 percentage_graduated,
                 percentage_youth_graduated
                 ) -> None:
        self.year = year
        self.id_country = id_country
        self.num_graduated_male = num_graduated_male
        self.num_graduated_female = num_graduated_female
        self.num_graduated = num_graduated
        self.percentage_graduated = percentage_graduated
        self.percentage_youth_graduated = percentage_youth_graduated





# DROP TABLE IF EXISTS fact_egresados_rama_enseñanza;
# CREATE TABLE IF NOT EXISTS fact_egresados_rama_enseñanza (
# 	año INT NOT NULL,
# 	id_pais INT,
#     id_universidad INT,
#     id_rama_enseñanza INT,
#     num_egresados INT
# );