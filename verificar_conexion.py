import pyodbc

# Conectar a SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=big-data-migration.c3se2wo28ad4.us-east-2.rds.amazonaws.com;'
    'DATABASE=big-data-migration;'
    'UID=denan;'
    'PWD=Mancoeve22:)'
)
cursor = conn.cursor()

# Verificar la conexión ejecutando una consulta simple
cursor.execute("SELECT @@version;")
row = cursor.fetchone()
print(row)
