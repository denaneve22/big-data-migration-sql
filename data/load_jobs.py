import boto3
import pandas as pd
import pyodbc

# Configurar boto3 para acceder a S3
s3 = boto3.client('s3')
bucket_name = 'data-lake-globant'

# Conectar a SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=big-data-migration.c3se2wo28ad4.us-east-2.rds.amazonaws.com;'
    'DATABASE=big-data-migration;'
    'UID=denan;'
    'PWD=Mancoeve22:)'
)
cursor = conn.cursor()

# Definir columnas y tipos de datos para la tabla jobs
columns = ['id', 'job']
types = ['INT', 'NVARCHAR(255)']

# Función para cargar datos desde un CSV a una tabla SQL Server con merge, validaciones y carga en batches
def merge_csv_to_sql(file_name, table_name, columns, types, batch_size=1000):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(obj['Body'], names=columns, header=0)  # Asumir que el CSV tiene encabezados

    # Crear una lista para almacenar los registros válidos
    valid_rows = []

    for index, row in df.iterrows():
        id_value = row['id']
        job_value = row['job']
        
        # Validaciones
        if pd.isnull(id_value) or not isinstance(id_value, int):
            print(f"Registro omitido por ID no válido: {row}")
            continue
        
        if pd.isnull(job_value) or not isinstance(job_value, str):
            print(f"Registro omitido por valor de trabajo no válido: {row}")
            continue

        valid_rows.append((id_value, job_value))

        # Procesar en batches
        if len(valid_rows) >= batch_size:
            cursor.executemany(f"""
                MERGE INTO {table_name} AS target
                USING (VALUES (?, ?)) AS source (id, job)
                ON target.id = source.id
                WHEN MATCHED THEN 
                    UPDATE SET target.job = source.job
                WHEN NOT MATCHED THEN
                    INSERT (id, job) VALUES (source.id, source.job);
            """, valid_rows)
            conn.commit()
            valid_rows = []

    # Procesar cualquier registro restante
    if valid_rows:
        cursor.executemany(f"""
            MERGE INTO {table_name} AS target
            USING (VALUES (?, ?)) AS source (id, job)
            ON target.id = source.id
            WHEN MATCHED THEN 
                UPDATE SET target.job = source.job
            WHEN NOT MATCHED THEN
                INSERT (id, job) VALUES (source.id, source.job);
        """, valid_rows)
        conn.commit()

# Cargar datos en la tabla jobs
merge_csv_to_sql('jobs.csv', 'jobs', columns, types)
