import os
import sys
import pandas as pd
import fastavro
import pyodbc
from sqlalchemy import create_engine
import json

# Agregar el directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import get_sql_connection, get_s3_client, S3_BUCKET_NAME

def load_schema(table_name):
    schema_path = f'../schemas/{table_name}_schema.avsc'
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    return schema

def backup_table(table_name, backup_file_path):
    # Crear el directorio backups si no existe
    os.makedirs(os.path.dirname(backup_file_path), exist_ok=True)

    conn = get_sql_connection()
    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=big-data-migration.c3se2wo28ad4.us-east-2.rds.amazonaws.com;'
        'DATABASE=big-data-migration;'
        'UID=denan;'
        'PWD=Mancoeve22:)'
    )
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
    df = pd.read_sql_table(table_name, engine)
    records = df.to_dict(orient='records')

    schema = load_schema(table_name)
    
    with open(backup_file_path, 'wb') as out:
        fastavro.writer(out, schema, records)

    s3 = get_s3_client()
    s3.upload_file(backup_file_path, S3_BUCKET_NAME, os.path.basename(backup_file_path))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python backup.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]
    backup_table(table_name, f'backups/{table_name}_backup.avro')
