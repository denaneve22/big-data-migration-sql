# Big Data Migration Project

## Descripción
Este proyecto es una prueba de concepto para la migración de grandes volúmenes de datos a un nuevo sistema de base de datos SQL Server utilizando AWS. Incluye la carga de datos históricos desde archivos CSV, la creación de un servicio API REST para recibir nuevos datos, y la funcionalidad para respaldar y restaurar datos en formato AVRO.

## Estructura del Proyecto
- `api/`: Contiene el código de la función Lambda y sus dependencias.
- `backup_restore/`: Contiene scripts para respaldar y restaurar datos.
- `data/`: Contiene scripts para cargar datos desde archivos CSV.
- `config/`: Contiene configuraciones y credenciales (no subir a GitHub).
- `scripts/`: Contiene scripts SQL para configurar la base de datos.
- `.gitignore`: Archivos y directorios a ignorar por Git.
- `README.md`: Documentación del proyecto.
- `requirements.txt`: Dependencias generales del proyecto.

## Configuración

### Requisitos Previos
- Python 3.8 o superior
- AWS CLI configurado con las credenciales adecuadas
- Acceso a una instancia de SQL Server en AWS RDS
- Bucket de S3 para almacenar archivos CSV y respaldos

### Instalación de Dependencias
Instalar las dependencias necesarias para cada componente:
```sh
pip install -r requirements.txt
