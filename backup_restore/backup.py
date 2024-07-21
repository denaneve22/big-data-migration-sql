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

def respaldar_tabla(table_name, file_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    schema = {
        "type": "record",
        "name": table_name,
        "fields": [{"name": column[0], "type": "string"} for column in cursor.description]
    }
    with open(file_name, 'wb') as out:
        writer = fastavro.writer(out, schema, rows)
    s3.upload_file(file_name, bucket_name, file_name)

# Ejemplo de uso
respaldar_tabla('employees', 'employees.avro')
respaldar_tabla('departments', 'departments.avro')
respaldar_tabla('jobs', 'jobs.avro')
