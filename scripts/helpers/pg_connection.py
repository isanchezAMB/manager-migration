import psycopg2
from helpers.config import PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD

def get_pg_connection():
    """Crea y retorna una conexión a la base de datos PostgreSQL usando variables de entorno."""

    if not all([PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD]):
        raise ValueError("Missing one or more required environment variables: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD")
    return psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST
    )
