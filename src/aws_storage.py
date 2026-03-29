import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

import boto3

from .config import get_aws_settings, get_s3_settings


def _build_boto3_session():
    aws = get_aws_settings()
    session_kwargs: Dict[str, Any] = {}
    if aws["profile"]:
        session_kwargs["profile_name"] = aws["profile"]
    else:
        import os

        for env_key in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
            if os.environ.get(env_key, None) == "":
                os.environ.pop(env_key, None)
    return boto3.session.Session(**session_kwargs)


def build_request_id() -> str:
    return str(uuid4())


def build_request_fingerprint(
    source_system: str,
    endpoint_name: str,
    marketplace_country: str,
    request_params: Dict[str, Any],
) -> str:
    serialized = json.dumps(request_params or {}, sort_keys=True, default=str, ensure_ascii=False)
    raw = "|".join(
        [
            source_system or "",
            endpoint_name or "",
            marketplace_country or "",
            serialized,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_idempotency_key(row: Dict[str, Any]) -> str:
    request_metadata = row.get("request_metadata") or {}
    raw = "|".join(
        [
            str(row.get("source_system", "")),
            str(request_metadata.get("account", "default")),
            str(row.get("endpoint_name") or row.get("source_endpoint", "")),
            str(row.get("source_record_id") or row.get("business_id") or row.get("asin", "")),
            str(row.get("source_updated_at") or row.get("event_time") or row.get("record_create_timestamp")),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_s3_key(
    source_system: str,
    endpoint_name: str,
    ingest_date: Any,
    request_id: str,
    source_record_id: Optional[str],
    business_domain: Optional[str] = None,
    dataset_name: Optional[str] = None,
) -> str:
    s3 = get_s3_settings()
    date_value = ingest_date
    if isinstance(ingest_date, datetime):
        date_value = ingest_date.date()
    safe_record = source_record_id or "unknown"
    safe_business_domain = (business_domain or "external-data").strip().replace(" ", "_")
    safe_dataset_name = (dataset_name or endpoint_name or "generic").strip().replace(" ", "_")
    return (
        f"{s3['prefix'].strip('/')}/business_domain={safe_business_domain}/"
        f"source_system={source_system}/dataset_name={safe_dataset_name}/endpoint_name={endpoint_name}/"
        f"ingest_date={date_value}/request_id={request_id}/{safe_record}.json"
    )


def put_json_to_s3(payload: Dict[str, Any], key: str) -> Dict[str, Optional[str]]:
    s3 = get_s3_settings()
    bucket = s3["bucket"]
    if not bucket:
        return {"bucket": None, "key": None, "etag": None, "version_id": None}

    aws = get_aws_settings()
    session = _build_boto3_session()
    client = session.client("s3", region_name=aws["region"])
    extra_args: Dict[str, str] = {"ContentType": "application/json"}
    if s3["sse"]:
        extra_args["ServerSideEncryption"] = s3["sse"]

    response = client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
        **extra_args,
    )
    return {
        "bucket": bucket,
        "key": key,
        "etag": str(response.get("ETag", "")).strip('"') or None,
        "version_id": response.get("VersionId"),
    }
