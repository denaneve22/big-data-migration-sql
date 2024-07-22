import boto3
import pandas as pd
import pyodbc
from io import StringIO

# Configurar boto3 para acceder a S3
s3 = boto3.client('s3')
bucket_name = 'data-lake-globant'
csv_file_key = 'hired_employees.csv'  # Cambia esta clave al nombre correcto del archivo en el bucket

# Conectar a SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=big-data-migration.c3se2wo28ad4.us-east-2.rds.amazonaws.com;'
    'DATABASE=big-data-migration;'
    'UID=denan;'
    'PWD=Mancoeve22:)'
)
cursor = conn.cursor()

# Descargar el archivo CSV desde S3
obj = s3.get_object(Bucket=bucket_name, Key=csv_file_key)
csv_data = obj['Body'].read().decode('utf-8')
df = pd.read_csv(StringIO(csv_data), header=None, names=['id', 'name', 'datetime', 'department_id', 'job_id'])

# Imprimir los nombres de las columnas para depuración
print("Nombres de columnas del CSV:", df.columns)

# Realizar validaciones en los datos
df['name'] = df['name'].fillna('Unknown Employee')
df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce').fillna(pd.Timestamp('1900-01-01T00:00:00Z'))
df['department_id'] = df['department_id'].fillna(-1).astype(int)
df['job_id'] = df['job_id'].fillna(-1).astype(int)

# Verificar que department_id y job_id existan en las tablas correspondientes
valid_departments = pd.read_sql("SELECT id FROM departments", conn)['id'].tolist()
valid_jobs = pd.read_sql("SELECT id FROM jobs", conn)['id'].tolist()

df = df[df['department_id'].isin(valid_departments) & df['job_id'].isin(valid_jobs)]

# Crear tabla temporal
cursor.execute("""
    CREATE TABLE #TempEmployees (
        id INT,
        name NVARCHAR(255),
        datetime DATETIME,
        department_id INT,
        job_id INT
    )
""")
conn.commit()

# Insertar datos en la tabla temporal
for index, row in df.iterrows():
    cursor.execute("INSERT INTO #TempEmployees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)",
                   row['id'], row['name'], row['datetime'], row['department_id'], row['job_id'])
conn.commit()

# Insertar o actualizar los datos desde la tabla temporal a la tabla real
merge_query = """
    MERGE INTO employees AS target
    USING #TempEmployees AS source
    ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET target.name = source.name, target.datetime = source.datetime, target.department_id = source.department_id, target.job_id = source.job_id
    WHEN NOT MATCHED THEN
        INSERT (id, name, datetime, department_id, job_id) VALUES (source.id, source.name, source.datetime, source.department_id, source.job_id);
"""
cursor.execute(merge_query)
conn.commit()

# Eliminar la tabla temporal
cursor.execute("DROP TABLE #TempEmployees")
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
