# dw-airflow-training

Ejercicio de entrenamiento para poblar un DW por medio de Apache Airflow


## 1. Setup

1. Crear archivo `.env` con la variable AIRFLOW_UID
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Editar .env y añadir librerias requeridas
```
_PIP_ADDITIONAL_REQUIREMENTS=openpyxl
```

3. Crea una subcarpeta dentro de data que se llame `raw`, luego descomprime el archivo comprimido fuentes. Debe quedar asi
```
data
└── raw
    ├── 03003.xlsx
    ├── ISCED_2013.csv
    ├── SEGR1.csv
    ├── SEGR2.csv
    ├── educ_uoe_grad01.xlsx
    ├── educ_uoe_grad05.xlsx
    └── grad_5sc.csv
```

4. Inicializar Airflow
```
docker compose up airflow-init
```

5. Conectate a la base de datos MySQL y ejecuta la migración `create_tables.sql`, la cual puedes encontrar en la tabla migrations
