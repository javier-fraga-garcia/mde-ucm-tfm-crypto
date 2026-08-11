import logging
import duckdb

from serving.config import ServingSettings

logger = logging.getLogger(__name__)


def get_connection(settings: ServingSettings) -> duckdb.DuckDBPyConnection:
    """Crea una conexión DuckDB con las extensiones y conexiones necesarias.
    Carga las extensiones delta, httpfs y postgres, configura el acceso
    al endpoint S3 (Floci) y adjunta la base de datos de TimescaleDB.
    Args:
        settings: Configuración del servicio serving.
    Returns:
        Conexión DuckDB lista para sincronizar tablas.
    """
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{settings.s3_access_key}',
            SECRET '{settings.s3_secret_key}',
            ENDPOINT '{settings.s3_endpoint_url.replace("http://", "")}',
            URL_STYLE 'path',
            USE_SSL false
        );""")
    con.execute(
        f"ATTACH 'dbname={settings.timescale_database} user={settings.timescale_user} "
        f"password={settings.timescale_password} host={settings.timescale_host} "
        f"port={settings.timescale_port}' AS pg (TYPE postgres);"
    )
    return con


def _table_exists(con: duckdb.DuckDBPyConnection, delta_path: str) -> bool:
    """Comprueba que haya datos en el delta_path indicado.

    Args:
        con: Conexión DuckDB activa.
        delta_path: Ruta S3 de la tabla Delta origen.
    """
    try:
        con.execute(f"SELECT 1 FROM delta_scan('{delta_path}') LIMIT 1")
        return True
    except duckdb.IOException:
        return False


def sync_table(
    con: duckdb.DuckDBPyConnection,
    delta_path: str,
    pg_table: str,
    key_columns: list[str],
) -> bool:
    """Sincroniza una tabla Gold hacia su tabla equivalente en TimescaleDB.

    Inserta únicamente las filas cuya clave no exista ya en el destino.

    Args:
        con: Conexión DuckDB activa.
        delta_path: Ruta S3 de la tabla Delta origen.
        pg_table: Nombre de la tabla destino en TimescaleDB.
        key_columns: Columnas que forman la clave única de la tabla.
    Returns:
        True si la tabla se sincronizó correctamente, False en otro caso
    """
    if not _table_exists(con, delta_path):
        logger.warning(f"Tabla Delta no disponible: {delta_path}")
        return False
    key_condition = " AND ".join(f"gold.{col} = pg.{col}" for col in key_columns)
    con.execute(f"""
        INSERT INTO pg.{pg_table}
        SELECT * FROM delta_scan('{delta_path}') AS gold
        WHERE NOT EXISTS (
            SELECT 1 FROM pg.{pg_table} AS pg WHERE {key_condition}
        );
    """)
    return True
