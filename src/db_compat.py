from typing import Any, Iterable, List, Mapping

import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor

from .config import get_database_backend


def is_postgres_backend() -> bool:
    return get_database_backend() == "postgresql"


def get_dict_cursor(connection):
    if is_postgres_backend():
        return connection.cursor(cursor_factory=RealDictCursor)
    return connection.cursor(pymysql.cursors.DictCursor)


def ping_connection(connection) -> None:
    if is_postgres_backend():
        if getattr(connection, "closed", 0):
            raise RuntimeError("PostgreSQL connection is closed")
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        return
    connection.ping(reconnect=True)


def normalize_rows(rows: Iterable[Any]) -> List[dict]:
    normalized: List[dict] = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append(row)
        elif isinstance(row, Mapping):
            normalized.append(dict(row))
        else:
            normalized.append(dict(row))
    return normalized


def is_integrity_error(exc: Exception) -> bool:
    return isinstance(exc, (pymysql.err.IntegrityError, psycopg2.IntegrityError))
