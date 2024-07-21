import fastavro
import pyodbc
import boto3
import os

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=' + os.environ['DB_SERVER'] + ';'
    'DATABASE=' + os.environ['DB_NAME'] + ';'
    'UID=' + os.environ['DB_USER'] + ';'
    'PWD=' + os.environ['DB_PASSWORD']
)
cursor = conn.cursor()
s3 = boto3.client('s3')
bucket_name = 'nombre-del-bucket'

def restaurar_tabla(file_name, table_name):
    s3.download_file(bucket_name, file_name, file_name)
    with open(file_name, 'rb') as fo:
        reader = fastavro.reader(fo)
        for record in reader:
            cursor.execute(f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in record])})", tuple(record.values()))
    conn.commit()

# Ejemplo de uso
restaurar_tabla('employees.avro', 'employees')
restaurar_tabla('departments.avro', 'departments')
restaurar_tabla('jobs.avro', 'jobs')
