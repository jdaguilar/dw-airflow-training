
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


class Tipo_universidad(Base):

    __tablename__ = 'dimm_tipo_universidad'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    desc_tipo_universidad = Column(String, nullable=False)

    def __init__(self,desc_tipo_universidad) -> None:
        self.desc_tipo_universidad = desc_tipo_universidad


class Universidades(Base):

    __tablename__ = 'dimm_universidades'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    nombre_universidad = Column(String, nullable=False)
    tipo_universidad = Column(String, nullable=False)
    modalidad = Column(String, nullable=False)

    def __init__(self,
                 nombre_universidad,
                 tipo_universidad,
                 modalidad) -> None:
        self.nombre_universidad = nombre_universidad
        self.tipo_universidad = tipo_universidad
        self.modalidad = modalidad


class Rama_enseñanza(Base):

    __tablename__ = 'dimm_rama_enseanza'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(Integer, primary_key=True)
    nombre_rama = Column(String, nullable=False)

    def __init__(self, id, nombre_rama) -> None:
        self.id = id
        self.nombre_rama = nombre_rama


class Ambito_enseñanza(Base):

    __tablename__ = 'dimm_ambito_enseanza'
   # __table_args__ = {'schema': 'pragma_gsheets'}
    id = Column(String, primary_key=True)
    desc_ambito = Column(String, nullable=False)
    id_rama = Column(String, nullable=False)
    nombre_rama = Column(String, nullable=False)

    def __init__(self, 
                 id,
                 desc_ambito, 
                 id_rama, 
                 nombre_rama) -> None:
        self.id = id
        self.desc_ambito = desc_ambito
        self.id_rama = id_rama
        self.nombre_rama = nombre_rama

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