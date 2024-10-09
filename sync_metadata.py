import json
import os
import snowflake.connector
import logging
import sys

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Verificar variables de entorno
required_env_vars = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", 
    "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"
]

for var in required_env_vars:
    if not os.getenv(var):
        logger.error(f"La variable de entorno {var} no está definida. Saliendo.")
        sys.exit(1)

# Configuración de Snowflake usando variables de entorno
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

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
    
    # Verificar conexión
    logger.info("Conexión a Snowflake establecida.")
    return conn

# Función para crear la tabla si no existe
def create_table_if_not_exists(conn, table_name, create_sql):
    full_table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE OR REPLACE TABLE {full_table_name} (
            {create_sql}
        );
        """)
        logger.info(f"Tabla {full_table_name} verificada/creada.")

# Función para sincronizar los datos desde un archivo JSON a una tabla
def sync_json_to_snowflake_table(conn, json_file, table_name, key_columns):
    full_table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
    
    with open(json_file, 'r') as file:
        data = json.load(file)

    with conn.cursor() as cur:
        for record in data:
            keys = ', '.join(record.keys())
            values = ', '.join([f"'{v}'" for v in record.values()])

            # Verificar si la clave es compuesta
            if isinstance(key_columns, list):
                # Si es una clave compuesta, construir el WHERE para todas las columnas de la clave
                on_clause = ' AND '.join([f"{full_table_name}.{key} = incoming.{key}" for key in key_columns])
            else:
                # Si no es una clave compuesta, usar la única columna de clave
                on_clause = f"{full_table_name}.{key_columns} = incoming.{key_columns}"

            # Comando MERGE para insertar o actualizar
            sql = f"""
            MERGE INTO {full_table_name} USING (SELECT {values}) AS incoming({keys})
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {', '.join([f"{k} = incoming.{k}" for k in record.keys()])}
            WHEN NOT MATCHED THEN INSERT ({keys}) VALUES ({values});
            """
            cur.execute(sql)
        logger.info(f"Datos sincronizados en la tabla {full_table_name} desde {json_file}.")

# Función principal para sincronizar todos los metadatos
def sync_metadata():
    conn = get_snowflake_connection()

    try:
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
                    nombre_caso STRING,
                    area_negocio STRING,
                    activo BOOLEAN
                """,
                "key_column": "id_caso_uso"
            },
            {
                "json_file": "orquestadores.json",
                "table_name": "orquestadores",
                "create_sql": """
                    id_orquestador STRING PRIMARY KEY,
                    nombre_orquestador STRING,
                    activo BOOLEAN
                """,
                "key_column": "id_orquestador"
            },
            {
                "json_file": "origenes_orquestadores.json",
                "table_name": "origenes_orquestadores",
                "create_sql": """
                    id_orquestador STRING,
                    id_origen STRING,
                    PRIMARY KEY (id_orquestador, id_origen),
                    FOREIGN KEY (id_orquestador) REFERENCES orquestadores(id_orquestador),
                    FOREIGN KEY (id_origen) REFERENCES origenes(id_origen)
                """,
                "key_columns": ["id_orquestador", "id_origen"]
            }
        ]
        
        # Procesar cada metadata
        for metadata in metadatas:
            create_table_if_not_exists(conn, metadata["table_name"], metadata["create_sql"])
            # Aquí se verifica si existe "key_columns", si no se utiliza "key_column"
            sync_json_to_snowflake_table(
                conn, 
                metadata["json_file"], 
                metadata["table_name"], 
                metadata.get("key_columns", metadata.get("key_column"))
            )

    except Exception as e:
        logger.error(f"Error durante la sincronización de metadatos: {e}")
    finally:
        conn.close()
        logger.info("Conexión a Snowflake cerrada.")

if __name__ == "__main__":
    sync_metadata()
