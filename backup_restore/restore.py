import os
import sys
import pyodbc
import fastavro
import pandas as pd
from sqlalchemy import create_engine

# Añadir el path del directorio padre para que se pueda importar el módulo config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import get_sql_connection, get_s3_client, S3_BUCKET_NAME

def restore_table(table_name, backup_file_path):
    # Leer el archivo AVRO
    with open(backup_file_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = [record for record in reader]
    
    # Convertir los registros en un DataFrame de pandas
    df = pd.DataFrame(records)
    
    # Conectar a la base de datos
    conn_str = (
        "mssql+pyodbc://denan:Mancoeve22:)@big-data-migration.c3se2wo28ad4.us-east-2.rds.amazonaws.com:1433/"
        "big-data-migration?driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(conn_str)
    
    # Insertar los datos en la tabla correspondiente
    df.to_sql(table_name, engine, if_exists='replace', index=False)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python restore.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]
    backup_file_path = f'backups/{table_name}_backup.avro'
    restore_table(table_name, backup_file_path)
