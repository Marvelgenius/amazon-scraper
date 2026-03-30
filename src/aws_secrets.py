import json
import logging
import os
from typing import Any, Dict

import boto3

from .config import get_aws_settings, get_postgres_settings


# Keep actionable AWS warnings/errors, but suppress repetitive credential discovery logs.
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)


def _build_boto3_session():
    aws = get_aws_settings()
    session_kwargs: Dict[str, Any] = {}
    if aws["profile"]:
        session_kwargs["profile_name"] = aws["profile"]
    else:
        for env_key in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
            if os.environ.get(env_key, None) == "":
                os.environ.pop(env_key, None)
    return boto3.session.Session(**session_kwargs)


def _coalesce_secret_value(secret_dict: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = secret_dict.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def fetch_secret(secret_name: str, region_name: str) -> Dict[str, Any]:
    session = _build_boto3_session()
    client = session.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_text = response.get("SecretString")
    if not secret_text:
        raise ValueError(f"Secret '{secret_name}' does not contain SecretString content.")
    parsed = json.loads(secret_text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Secret '{secret_name}' is not a JSON object.")
    return parsed


_cached_credentials: Dict[str, Any] = {}


def resolve_postgres_credentials() -> Dict[str, Any]:
    if _cached_credentials:
        return dict(_cached_credentials)

    base = get_postgres_settings()
    aws = get_aws_settings()
    secret_name = aws["secret_name"]

    if not secret_name:
        return base

    try:
        secret = fetch_secret(secret_name=secret_name, region_name=aws["region"])
    except Exception:
        if not aws["secret_fallback_to_env"]:
            raise
        return base

    resolved = dict(base)
    resolved["host"] = _coalesce_secret_value(secret, "host", "hostname") or resolved["host"]
    resolved["port"] = int(_coalesce_secret_value(secret, "port") or resolved["port"])
    resolved["database"] = (
        _coalesce_secret_value(secret, "dbname", "database", "dbInstanceIdentifier")
        or resolved["database"]
    )
    resolved["user"] = _coalesce_secret_value(secret, "username", "user") or resolved["user"]
    resolved["password"] = _coalesce_secret_value(secret, "password") or resolved["password"]
    _cached_credentials.update(resolved)
    return resolved
