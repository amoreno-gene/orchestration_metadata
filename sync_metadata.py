import json
import os
import snowflake.connector
import logging

# Configuración de Snowflake usando variables de entorno
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función para conectar a Snowflake
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

# Función para crear la tabla si no existe
def create_table_if_not_exists(conn, table_name, create_sql):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {create_sql}
        );
        """)
        logger.info(f"Tabla {table_name} verificada/creada.")

# Función para sincronizar los datos desde un archivo JSON a una tabla
def sync_json_to_snowflake_table(conn, json_file, table_name, key_column):
    with open(json_file, 'r') as file:
        data = json.load(file)

    with conn.cursor() as cur:
        for record in data['origenes']:
            keys = ', '.join(record.keys())
            values = ', '.join([f"'{v}'" for v in record.values()])

            # Comando MERGE para insertar o actualizar
            sql = f"""
            MERGE INTO {table_name} USING (SELECT {values}) AS incoming({keys})
            ON {table_name}.{key_column} = incoming.{key_column}
            WHEN MATCHED THEN UPDATE SET {', '.join([f"{k} = incoming.{k}" for k in record.keys()])}
            WHEN NOT MATCHED THEN INSERT ({keys}) VALUES ({values});
            """
            cur.execute(sql)
        logger.info(f"Datos sincronizados en la tabla {table_name} desde {json_file}.")

# Función principal para sincronizar todos los metadatos
def sync_metadata():
    conn = get_snowflake_connection()

    try:
        # Definir las tablas y columnas
        metadatas = [
            {
                "json_file": "origenes.json",
                "table_name": "origenes",
                "create_sql": """
                    id_origen STRING PRIMARY KEY,
                    nombre_origen STRING,
                    activo BOOLEAN,
                    id_caso_uso STRING
                """,
                "key_column": "id_origen"
            },
            {
                "json_file": "casos_uso.json",
                "table_name": "casos_uso",
                "create_sql": """
                    id_caso_uso STRING PRIMARY KEY,
                    nombre STRING,
                    activo BOOLEAN
                """,
                "key_column": "id_caso_uso"
            },
            {
                "json_file": "orquestadores.json",
                "table_name": "orquestadores",
                "create_sql": """
                    id_orquestador STRING PRIMARY KEY,
                    nombre STRING,
                    activo BOOLEAN
                """,
                "key_column": "id_orquestador"
            }
        ]

        # Crear las tablas si no existen y sincronizar los datos
        for metadata in metadatas:
            create_table_if_not_exists(conn, metadata["table_name"], metadata["create_sql"])
            sync_json_to_snowflake_table(conn, metadata["json_file"], metadata["table_name"], metadata["key_column"])

    finally:
        conn.close()

if __name__ == "__main__":
    sync_metadata()
