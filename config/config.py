import pyodbc
import boto3

# Configuración de conexión a SQL Server
def get_sql_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=big-data-migration.c3se2wo28ad4.us-east-2.rds.amazonaws.com;'
        'DATABASE=big-data-migration;'
        'UID=denan;'
        'PWD=Mancoeve22:)'
    )
    return conn

# Configuración de conexión a S3
def get_s3_client():
    s3 = boto3.client('s3', region_name='us-east-2')
    return s3

# Variables de configuración
S3_BUCKET_NAME = 'data-lake-globant'
