"""
utils/db.py — Gestión de conexiones a SQL Server
"""

import pyodbc
import config
from utils.logger import get_logger

log = get_logger(__name__)


def _build_conn_str(server: str, database: str) -> str:
    if config.USE_WINDOWS_AUTH:
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={config.SQL_USER};PWD={config.SQL_PASS};"
    )


def get_connection() -> pyodbc.Connection:
    """Conexión a la BD transaccional (OpClientes)"""
    try:
        conn = pyodbc.connect(_build_conn_str(config.SERVER, config.DATABASE))
        log.debug(f"Conexión establecida: {config.DATABASE}")
        return conn
    except pyodbc.Error as e:
        log.error(f"Error conectando a {config.DATABASE}: {e}")
        raise


def get_dw_connection() -> pyodbc.Connection:
    """Conexión a la BD analítica (OpClientes_DW)"""
    try:
        conn = pyodbc.connect(_build_conn_str(config.DW_SERVER, config.DW_DATABASE))
        log.debug(f"Conexión establecida: {config.DW_DATABASE}")
        return conn
    except pyodbc.Error as e:
        log.error(f"Error conectando a {config.DW_DATABASE}: {e}")
        raise
