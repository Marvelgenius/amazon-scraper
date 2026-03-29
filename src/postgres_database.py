from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
from sshtunnel import SSHTunnelForwarder

from .aws_secrets import resolve_postgres_credentials
from .config import get_ssh_tunnel_settings

_tunnel: Optional[SSHTunnelForwarder] = None
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def create_postgres_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    use_ssh_tunnel: Optional[bool] = None,
):
    settings = resolve_postgres_credentials()
    resolved = {
        "host": host or settings["host"],
        "port": port or settings["port"],
        "user": user or settings["user"],
        "password": password or settings["password"],
        "dbname": database or settings["database"],
        "sslmode": settings["sslmode"],
        "application_name": settings["application_name"],
    }
    ssh = get_ssh_tunnel_settings()
    should_tunnel = use_ssh_tunnel if use_ssh_tunnel is not None else bool(ssh["ssh_host"])
    if should_tunnel:
        return _connect_via_ssh_tunnel(resolved, ssh)
    return psycopg2.connect(**resolved)


def _connect_via_ssh_tunnel(
    pg_settings: Dict[str, Any],
    ssh_settings: Dict[str, Any],
):
    global _tunnel
    if _tunnel is None or not _tunnel.is_active:
        _tunnel = SSHTunnelForwarder(
            (ssh_settings["ssh_host"], ssh_settings["ssh_port"]),
            ssh_username=ssh_settings["ssh_username"],
            ssh_pkey=ssh_settings["ssh_pkey"],
            remote_bind_address=(pg_settings["host"], int(pg_settings["port"])),
        )
        _tunnel.start()

    connect_kwargs = dict(pg_settings)
    connect_kwargs["host"] = "127.0.0.1"
    connect_kwargs["port"] = _tunnel.local_bind_port
    return psycopg2.connect(**connect_kwargs)


def close_postgres_tunnel() -> None:
    global _tunnel
    if _tunnel is not None and _tunnel.is_active:
        _tunnel.stop()
        _tunnel = None


def execute_sql_file(connection, path: Path) -> None:
    with path.open("r", encoding="utf-8") as handle:
        sql = handle.read()
    if not sql.strip():
        return
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def bulk_insert_raw_events(connection, rows: Iterable[Dict[str, Any]]) -> int:
    normalized_rows = list(rows)
    if not normalized_rows:
        return 0

    sql = """
        INSERT INTO raw.api_ingest_event (
            source_system,
            endpoint_name,
            request_id,
            request_fingerprint,
            ingest_date,
            fetched_at,
            marketplace_country,
            account_name,
            source_record_id,
            source_updated_at,
            business_id,
            idempotency_key,
            request_params_json,
            response_body_jsonb,
            s3_bucket,
            s3_key,
            s3_etag,
            s3_version_id
        ) VALUES %s
        ON CONFLICT (ingest_date, idempotency_key) DO UPDATE SET
            fetched_at = EXCLUDED.fetched_at,
            request_params_json = EXCLUDED.request_params_json,
            response_body_jsonb = EXCLUDED.response_body_jsonb,
            s3_bucket = EXCLUDED.s3_bucket,
            s3_key = EXCLUDED.s3_key,
            s3_etag = EXCLUDED.s3_etag,
            s3_version_id = EXCLUDED.s3_version_id
    """

    values = [
        (
            row["source_system"],
            row["endpoint_name"],
            row["request_id"],
            row["request_fingerprint"],
            row["ingest_date"],
            row["fetched_at"],
            row["marketplace_country"],
            row["account_name"],
            row.get("source_record_id"),
            row.get("source_updated_at"),
            row.get("business_id"),
            row["idempotency_key"],
            Json(row.get("request_params_json") or {}),
            Json(row["response_body_jsonb"]),
            row.get("s3_bucket"),
            row.get("s3_key"),
            row.get("s3_etag"),
            row.get("s3_version_id"),
        )
        for row in normalized_rows
    ]

    with connection.cursor() as cursor:
        execute_values(cursor, sql, values, page_size=200)
    connection.commit()
    return len(normalized_rows)
