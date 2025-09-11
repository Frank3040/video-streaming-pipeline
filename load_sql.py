
import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
USERS_CSV = os.getenv("USERS_CSV")
SESSIONS_CSV = os.getenv("SESSIONS_CSV")

BATCH_SIZE = 500  # tamaño del batch


def crear_base_datos():
    """Crea la base de datos si no existe."""
    try:
        # Conexión a la BD 'postgres' que siempre existe
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname="postgres",
            user=PG_USER,
            password=PG_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Verificar si la base ya existe
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (PG_DB,))
        existe = cur.fetchone()

        if not existe:
            cur.execute(f"CREATE DATABASE {PG_DB};")
            print(f"Base de datos '{PG_DB}' creada.")
        else:
            print(f"ℹLa base de datos '{PG_DB}' ya existe.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error creando la base de datos: {e}")


def crear_tablas(conn):
    """Crear tablas normalizadas en PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR PRIMARY KEY,
                age INT,
                country VARCHAR(50),
                subscription_type VARCHAR(20),
                registration_date DATE,
                total_watch_time_hours NUMERIC
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS viewing_sessions (
                session_id VARCHAR PRIMARY KEY,
                user_id VARCHAR REFERENCES users(user_id),
                content_id VARCHAR,
                watch_date DATE,
                watch_duration_minutes INT,
                completion_percentage NUMERIC,
                device_type VARCHAR(50),
                quality_level VARCHAR(20)
            );
        """)
    conn.commit()


def insertar_en_batches(conn, df, tabla, columnas):
    """Inserta datos en batches en la tabla indicada."""
    registros = df.to_records(index=False).tolist()
    with conn.cursor() as cur:
        for i in range(0, len(registros), BATCH_SIZE):
            batch = registros[i:i+BATCH_SIZE]
            try:
                execute_values(
                    cur,
                    f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES %s ON CONFLICT DO NOTHING;",
                    batch
                )
            except Exception as e:
                print(f"❌ Error insertando batch en {tabla}: {e}")
    conn.commit()
    print(f"✅ Insertados {len(registros)} registros en {tabla}.")


def main():
    try:
        # Paso 1: Crear la base si no existe
        crear_base_datos()

        # Paso 2: Conectar a la base creada
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("✅ Conectado a PostgreSQL.")

        # Paso 3: Crear tablas
        crear_tablas(conn)

        # Paso 4: Cargar datos Users
        users_df = pd.read_csv(USERS_CSV)
        users_columns = ["user_id", "age", "country", "subscription_type",
                         "registration_date", "total_watch_time_hours"]
        insertar_en_batches(conn, users_df, "users", users_columns)

        # Paso 5: Cargar datos Viewing Sessions
        sessions_df = pd.read_csv(SESSIONS_CSV)
        sessions_columns = ["session_id", "user_id", "content_id", "watch_date",
                            "watch_duration_minutes", "completion_percentage",
                            "device_type", "quality_level"]
        insertar_en_batches(conn, sessions_df, "viewing_sessions", sessions_columns)

        conn.close()
        print("✅ Proceso completado correctamente.")

    except Exception as e:
        print(f"❌ Error general: {e}")


if __name__ == "__main__":
    main()

