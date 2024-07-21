import json
import pyodbc
import os

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=' + os.environ['DB_SERVER'] + ';'
    'DATABASE=' + os.environ['DB_NAME'] + ';'
    'UID=' + os.environ['DB_USER'] + ';'
    'PWD=' + os.environ['DB_PASSWORD']
)
cursor = conn.cursor()

def lambda_handler(event, context):
    data = json.loads(event['body'])
    table_name = data['table']
    rows = data['rows']
    columns = data['columns']
    for row in rows:
        cursor.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in row])})", tuple(row))
    conn.commit()
    return {
        'statusCode': 200,
        'body': json.dumps('Datos insertados correctamente')
    }
