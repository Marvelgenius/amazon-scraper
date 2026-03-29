import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

DEFAULT_RAPIDAPI_HOST = "real-time-amazon-data.p.rapidapi.com"
DEFAULT_RAPIDAPI_BASE_URL = f"https://{DEFAULT_RAPIDAPI_HOST}"
DEFAULT_DB_BACKEND = "mysql"
POSTGRES_BACKENDS = {"postgres", "postgresql"}


def _clean_env(value: Optional[str], default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip().strip('"').strip("'")


def _clean_env_or_default(value: Optional[str], default: str) -> str:
    cleaned = _clean_env(value, default=default)
    return cleaned if cleaned else default


def _expand_path(path_value: str) -> str:
    expanded = _clean_env(path_value)
    home = os.path.expanduser("~")
    if expanded.startswith("$HOME/"):
        expanded = os.path.join(home, expanded[len("$HOME/"):])
    elif expanded == "$HOME":
        expanded = home
    elif expanded.startswith("${HOME}/"):
        expanded = os.path.join(home, expanded[len("${HOME}/"):])
    elif expanded == "${HOME}":
        expanded = home
    return os.path.expanduser(os.path.expandvars(expanded))


def get_rapidapi_settings(api_key: Optional[str] = None) -> Dict[str, Any]:
    resolved_api_key = api_key or os.getenv("RAPIDAPI_KEY")
    if not resolved_api_key:
        raise ValueError(
            "RapidAPI key is required. Set RAPIDAPI_KEY or pass key/api_key explicitly."
        )

    return {
        "api_key": resolved_api_key,
        "api_host": os.getenv("RAPIDAPI_HOST", DEFAULT_RAPIDAPI_HOST),
        "base_url": os.getenv("RAPIDAPI_BASE_URL", DEFAULT_RAPIDAPI_BASE_URL),
        "timeout": int(os.getenv("RAPIDAPI_TIMEOUT", "30")),
    }


def get_database_backend() -> str:
    backend = _clean_env(os.getenv("DB_DIALECT", os.getenv("DATABASE_BACKEND", DEFAULT_DB_BACKEND)))
    normalized = (backend or DEFAULT_DB_BACKEND).strip().lower()
    return "postgresql" if normalized in POSTGRES_BACKENDS else "mysql"


def get_database_settings() -> Dict[str, Any]:
    return {
        "backend": "mysql",
        "host": os.getenv("DB_HOST", os.getenv("AWS_SE1B_RDS_01_END_POINT", "")),
        "port": int(os.getenv("DB_PORT", "3306")),
        "database": os.getenv("DB_NAME", "gurysk_src"),
        "user": os.getenv("DB_USER", os.getenv("AWS_SE1B_RDS_01_USER", "")),
        "password": os.getenv("DB_PASSWORD", os.getenv("AWS_SE1B_RDS_01_PWD", "")),
        "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    }


def get_postgres_settings() -> Dict[str, Any]:
    return {
        "backend": "postgresql",
        "host": _clean_env_or_default(os.getenv("PGHOST", os.getenv("DB_HOST", "")), ""),
        "port": int(os.getenv("PGPORT", os.getenv("DB_PORT", "5432"))),
        "database": _clean_env_or_default(
            os.getenv("PGDATABASE", os.getenv("DB_NAME", "ecommerce_platform")),
            "ecommerce_platform",
        ),
        "user": _clean_env_or_default(os.getenv("PGUSER", os.getenv("DB_USER", "")), ""),
        "password": _clean_env_or_default(os.getenv("PGPASSWORD", os.getenv("DB_PASSWORD", "")), ""),
        "sslmode": _clean_env_or_default(os.getenv("PGSSLMODE", "prefer"), "prefer"),
        "application_name": _clean_env_or_default(os.getenv(
            "PGAPPNAME",
            "ecommerce-data-collector-analysis",
        ), "ecommerce-data-collector-analysis"),
    }


def get_aws_settings() -> Dict[str, Any]:
    return {
        "region": _clean_env(os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"))),
        "profile": _clean_env(os.getenv("AWS_PROFILE", "")),
        "secret_name": _clean_env(os.getenv("AWS_SECRET_NAME", "")),
        "secret_fallback_to_env": os.getenv("AWS_SECRET_FALLBACK_TO_ENV", "true").lower() == "true",
    }


def get_s3_settings() -> Dict[str, Any]:
    return {
        "bucket": _clean_env(os.getenv("S3_RAW_BUCKET", "")),
        "prefix": _clean_env(os.getenv("S3_RAW_PREFIX", "raw")),
        "sse": _clean_env(os.getenv("S3_SERVER_SIDE_ENCRYPTION", "")),
    }


def get_ssh_tunnel_settings() -> Dict[str, Any]:
    ssh_host = _clean_env(os.getenv(
        "BASTION_HOST",
        os.getenv("SSH_HOST", os.getenv("AWS_SE1B_CICD_HOST", "")),
    ))
    ssh_port = _clean_env(os.getenv(
        "BASTION_PORT",
        os.getenv("SSH_PORT", os.getenv("AWS_SE1B_CICD_SSH_PORT", "22")),
    ))
    ssh_username = _clean_env(os.getenv(
        "BASTION_USER",
        os.getenv("SSH_USERNAME", os.getenv("AWS_SE1B_CICD_USERNAME", "ec2-user")),
    ))
    ssh_pkey_raw = os.getenv(
        "BASTION_SSH_PKEY",
        os.getenv("SSH_PKEY", os.path.expanduser("~/.ssh/id_rsa")),
    )
    ssh_pkey = _expand_path(ssh_pkey_raw)
    return {
        "ssh_host": ssh_host,
        "ssh_port": int(ssh_port),
        "ssh_username": ssh_username,
        "ssh_pkey": ssh_pkey,
        "required": os.getenv("DB_REQUIRE_SSH", "true").lower() == "true",
    }


def get_active_database_settings() -> Dict[str, Any]:
    if get_database_backend() == "postgresql":
        return get_postgres_settings()
    return get_database_settings()


def get_rds_conn() -> Dict[str, Any]:
    """
    Build a connection config dict for downstream tools.

    Legacy callers still expect the MySQL shape, so PostgreSQL support is
    additive and only used by the refactored data platform path.
    """
    backend = get_database_backend()
    ssh = get_ssh_tunnel_settings()
    if backend == "postgresql":
        db = get_postgres_settings()
        conn: Dict[str, Any] = {
            "client_type": "postgres",
            "host": db["host"],
            "port": db["port"],
            "user": db["user"],
            "password": db["password"],
            "database": db["database"],
            "sslmode": db["sslmode"],
        }
        if ssh["ssh_host"]:
            conn.update(
                {
                    "ssh_host": ssh["ssh_host"],
                    "ssh_port": ssh["ssh_port"],
                    "ssh_username": ssh["ssh_username"],
                    "ssh_pkey": ssh["ssh_pkey"],
                }
            )
        return conn

    db = get_database_settings()
    if ssh["ssh_host"]:
        return {
            "client_type": "mysqlssh",
            "db_host": db["host"],
            "db_port": db["port"],
            "db_user": db["user"],
            "db_password": db["password"],
            "db_database": db["database"],
            "ssh_host": ssh["ssh_host"],
            "ssh_port": ssh["ssh_port"],
            "ssh_username": ssh["ssh_username"],
            "ssh_pkey": ssh["ssh_pkey"],
        }

    return {
        "client_type": "mysql",
        "host": db["host"],
        "port": db["port"],
        "user": db["user"],
        "password": db["password"],
        "database": db["database"],
    }


# Module-level config dicts consumed by dashboard/pages/config.py and others.
_db = get_active_database_settings()
_ssh = get_ssh_tunnel_settings()

DB_CONFIG: Dict[str, Any] = {
    "backend": get_database_backend(),
    "host": _db["host"],
    "port": _db["port"],
    "user": _db["user"],
    "password": _db["password"],
    "database": _db["database"],
}

SSH_CONFIG: Dict[str, Any] = {
    "enabled": bool(_ssh["ssh_host"]),
    "host": _ssh["ssh_host"],
    "port": _ssh["ssh_port"],
    "user": _ssh["ssh_username"],
    "key_file": _ssh["ssh_pkey"],
}
