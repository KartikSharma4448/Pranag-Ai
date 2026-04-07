from __future__ import annotations

from pathlib import Path
from typing import Any

from universal_index.config import (
    DATA_DIR,
    OBJECT_STORAGE_ACCESS_KEY_ID,
    OBJECT_STORAGE_BUCKET,
    OBJECT_STORAGE_ENABLED,
    OBJECT_STORAGE_ENDPOINT_URL,
    OBJECT_STORAGE_REGION,
    OBJECT_STORAGE_S3_ADDRESSING_STYLE,
    OBJECT_STORAGE_SECRET_ACCESS_KEY,
    OBJECT_STORAGE_SESSION_TOKEN,
    OBJECT_STORAGE_SSE,
)

try:
    import boto3
    from botocore.config import Config as BotoConfig
except ImportError:  # pragma: no cover - optional dependency
    boto3 = None
    BotoConfig = None


class ObjectStorageClient:
    def __init__(
        self,
        bucket: str,
        endpoint_url: str,
        region: str,
        access_key_id: str,
        secret_access_key: str,
        session_token: str,
        addressing_style: str,
        sse: str,
    ) -> None:
        if boto3 is None or BotoConfig is None:
            raise RuntimeError("Object storage is enabled but boto3 is not installed.")

        self.bucket = bucket
        self.sse = sse
        self.data_root = DATA_DIR.resolve()
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or None,
            region_name=region or None,
            aws_access_key_id=access_key_id or None,
            aws_secret_access_key=secret_access_key or None,
            aws_session_token=session_token or None,
            config=BotoConfig(s3={"addressing_style": addressing_style or "auto"}),
        )

    def build_object_key(self, local_path: str | Path, run_id: str) -> str:
        path = Path(local_path).resolve()
        try:
            relative = path.relative_to(self.data_root)
            relative_part = relative.as_posix()
        except ValueError:
            relative_part = path.name
        return f"runs/{run_id}/{relative_part}"

    def upload_file(self, local_path: str | Path, object_key: str) -> str:
        extra_args: dict[str, Any] = {}
        if self.sse:
            extra_args["ServerSideEncryption"] = self.sse

        if extra_args:
            self.s3_client.upload_file(str(local_path), self.bucket, object_key, ExtraArgs=extra_args)
        else:
            self.s3_client.upload_file(str(local_path), self.bucket, object_key)

        return f"s3://{self.bucket}/{object_key}"


def build_object_storage_client() -> ObjectStorageClient | None:
    if not OBJECT_STORAGE_ENABLED:
        return None
    if not OBJECT_STORAGE_BUCKET:
        return None

    return ObjectStorageClient(
        bucket=OBJECT_STORAGE_BUCKET,
        endpoint_url=OBJECT_STORAGE_ENDPOINT_URL,
        region=OBJECT_STORAGE_REGION,
        access_key_id=OBJECT_STORAGE_ACCESS_KEY_ID,
        secret_access_key=OBJECT_STORAGE_SECRET_ACCESS_KEY,
        session_token=OBJECT_STORAGE_SESSION_TOKEN,
        addressing_style=OBJECT_STORAGE_S3_ADDRESSING_STYLE,
        sse=OBJECT_STORAGE_SSE,
    )
