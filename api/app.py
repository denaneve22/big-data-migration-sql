from flask import Flask, request, jsonify
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


app = Flask(__name__)

# Validación de datos
def validate_data(data, table_name):
    if table_name == "departments":
        for record in data:
            if not isinstance(record['id'], int) or not isinstance(record['department'], str):
                return False
    elif table_name == "employees":
        for record in data:
            if not isinstance(record['id'], int) or not isinstance(record['employee_name'], str):
                return False
    elif table_name == "jobs":
        for record in data:
            if not isinstance(record['id'], int) or not isinstance(record['job'], str):
                return False
    return True

# Ruta para insertar datos en una tabla específica
@app.route('/insert/<table_name>', methods=['POST'])
def insert_data(table_name):
    if table_name not in ["departments", "employees", "jobs"]:
        return jsonify({"error": "Invalid table name"}), 400

    data = request.json
    if not validate_data(data, table_name):
        return jsonify({"error": "Invalid data format"}), 400

    conn = get_sql_connection()
    cursor = conn.cursor()

    if table_name == "departments":
        query = "INSERT INTO departments (id, department) VALUES (?, ?)"
    elif table_name == "employees":
        query = "INSERT INTO employees (id, employee_name) VALUES (?, ?)"
    elif table_name == "jobs":
        query = "INSERT INTO jobs (id, job) VALUES (?, ?)"

    for record in data:
        cursor.execute(query, (record['id'], record[table_name[:-1]]))

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"message": f"{len(data)} records inserted into {table_name}"}), 201

if __name__ == '__main__':
    print("Iniciando la aplicación Flask en http://127.0.0.1:5000")
    app.run(debug=True)
