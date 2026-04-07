# Storage Decision Record

Date: 2026-04-07

## Decision

Use S3-compatible object storage model:
- Production: S3 (or cloud equivalent)
- Local/Dev: MinIO

## Reason

- Single code path for dev and production.
- Existing integration in repo already uses S3-compatible client.
- Easy migration across providers.

## Repo Support

- Object storage client: universal_index/storage.py
- Ingestion upload wiring: universal_index/distributed_ingest.py
- Environment controls: .env.example

## Required Environment

- OBJECT_STORAGE_ENABLED=true
- OBJECT_STORAGE_BUCKET=<bucket-name>
- OBJECT_STORAGE_ENDPOINT_URL=<minio-endpoint for dev>
- OBJECT_STORAGE_ACCESS_KEY_ID=<key>
- OBJECT_STORAGE_SECRET_ACCESS_KEY=<secret>
