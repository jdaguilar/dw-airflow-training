
from sqlalchemy import Column, String, Integer, DateTime, Float, Date, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Date,Boolean
Base = declarative_base()

#dimensiones

class Pais(Base):

    __tablename__ = 'dimm_pais'

    id = Column(Integer, primary_key=True)
    nombre_pais = Column(String, nullable=False)

    def __init__(self, nombre_pais) -> None:
        self.nombre_pais = nombre_pais


class Sexo(Base):

    __tablename__ = 'dimm_sexo'

    id = Column(Integer, primary_key=True)
    desc_sexo = Column(String, nullable=False)

    def __init__(self,desc_sexo) -> None:
        self.desc_sexo = desc_sexo

class Situacion_laboral(Base):

    __tablename__ = 'dimm_situacion_laboral'

    id = Column(Integer, primary_key=True)
    desc_situacion_laboral = Column(String, nullable=False)

    def __init__(self,desc_situacion_laboral) -> None:
        self.desc_situacion_laboral = desc_situacion_laboral


class Rango_edad(Base):


    __tablename__ = 'dimm_rango_edad'

    id = Column(Integer, primary_key=True)
    desc_rango_edad = Column(String, nullable=False)

    def __init__(self,desc_rango_edad) -> None:
        self.desc_rango_edad = desc_rango_edad


class Tipo_universidad(Base):

    __tablename__ = 'dimm_tipo_universidad'

    id = Column(Integer, primary_key=True)
    desc_tipo_universidad = Column(String, nullable=False)

    def __init__(self,desc_tipo_universidad) -> None:
        self.desc_tipo_universidad = desc_tipo_universidad


class Universidades(Base):

    __tablename__ = 'dimm_universidades'

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

    id = Column(Integer, primary_key=True)
    nombre_rama = Column(String, nullable=False)

    def __init__(self, id, nombre_rama) -> None:
        self.id = id
        self.nombre_rama = nombre_rama


class Ambito_enseñanza(Base):

    __tablename__ = 'dimm_ambito_enseanza'

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

    __tablename__ = 'fact_international_graduated'

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


class Fact_egresados_niveles(Base):

    __tablename__ = 'fact_egresados_niveles'

    year = Column(Integer, primary_key=True)
    id_pais = Column(Integer, primary_key=True)
    id_ambito = Column(String, nullable=False)
    id_sexo = Column(Integer, primary_key=True)
    id_rango_edad = Column(Integer, nullable=False)
    num_egresados_nivel_1 = Column(Integer, nullable=False)
    num_egresados_nivel_2 = Column(Integer, nullable=False)
    num_egresados = Column(Integer, nullable=False)

    def __init__(self,
                 year,
                 id_pais,
                 id_ambito,
                 id_sexo,
                 id_rango_edad,
                 num_egresados_nivel_1,
                 num_egresados_nivel_2,
                 num_egresados
    ) -> None:

        self.year = year
        self.id_pais = id_pais
        self.id_ambito = id_ambito
        self.id_sexo = id_sexo
        self.id_rango_edad = id_rango_edad
        self.num_egresados_nivel_1 = num_egresados_nivel_1
        self.num_egresados_nivel_2 = num_egresados_nivel_2
        self.num_egresados = num_egresados


class Fact_situacion_laboral_egresados(Base):

    __tablename__ = 'fact_situacion_laboral_egresados'

    year = Column(Integer, nullable=False)
    id_pais = Column(Integer, primary_key=True)
    id_tipo_universidad = Column(Integer, nullable=False)
    id_area_estudio = Column(String, nullable=False)
    id_sexo = Column(Integer, nullable=False)
    id_situacion_laboral = Column(Integer, nullable=False)
    cantidad = Column(Integer, nullable=False)

    def __init__(self,
                 year,
                 id_pais,
                 id_tipo_universidad,
                 id_area_estudio,
                 id_sexo,
                 id_situacion_laboral,
                 cantidad
    ) -> None:

            self.year = year
            self.id_pais = id_pais
            self.id_tipo_universidad = id_tipo_universidad
            self.id_area_estudio = id_area_estudio
            self.id_sexo = id_sexo
            self.id_situacion_laboral = id_situacion_laboral
            self.cantidad = cantidad


