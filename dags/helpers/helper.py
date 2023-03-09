import pandas as pd


def preprocesar_archivo_situacion_laboral(file: str, directory: str):

    filepath = directory + file

    raw_df = pd.read_excel(
        filepath,
        header=6,
        skiprows=lambda x: x in range(7, 29) or x in range(
            30, 37) or x in range(52, 59) or x in [38, 45, 60, 67],
        usecols='A, C:E',
        skipfooter=9,
        names=['area_estudio', 'trabajando', 'en desempleo', 'inactivo']
    )

    raw_df['area_estudio'] = raw_df['area_estudio'].str.strip()

    men_df = raw_df.iloc[0:13, :].copy()
    men_df['sexo'] = 'Masculino'
    men_public_df = men_df.iloc[:7, :].dropna()
    men_public_df['tipo_universidad'] = 'Pública'
    men_private_df = men_df.iloc[7:, :].dropna()
    men_private_df['tipo_universidad'] = 'Privada'

    women_df = raw_df.iloc[13:, :].copy()
    women_df['sexo'] = 'Femenino'
    women_public_df = women_df.iloc[:7, :].dropna()
    women_public_df['tipo_universidad'] = 'Pública'
    women_private_df = women_df.iloc[7:, :].dropna()
    women_private_df['tipo_universidad'] = 'Privada'

    new_df = pd.concat([men_public_df, men_private_df,women_public_df, women_private_df], ignore_index=True)

    new_df = pd.melt(
        new_df,
        id_vars=['tipo_universidad', 'area_estudio', 'sexo'],
        value_vars=['trabajando', 'en desempleo', 'inactivo'],
        var_name='situacion_laboral',
        value_name='cantidad'
    )

    new_df['año'] = 2014
    new_df['pais'] = 'Spain'

    new_df[[
        'año', 'pais', 'tipo_universidad',	'area_estudio',	'sexo',	'situacion_laboral',	'cantidad'
    ]].to_csv('/tmp/data/processed/03003.csv', index=False)


def cargar_archivo_situacion_laboral():
    pass
