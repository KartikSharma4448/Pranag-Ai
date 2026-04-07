# Backup and Restore Policy

Updated: 2026-04-07

## Scope

Backup these assets daily:
- data/processed/*.duckdb
- data/processed/*.parquet
- data/processed/*.json
- data/processed/chroma/
- data/lake/

## Retention

- Daily backups: 7 days
- Weekly snapshot: 4 weeks

## Backup Command (Windows PowerShell)

Run from repo root:

scripts/backup_local.ps1

## Restore Command (Windows PowerShell)

Run from repo root:

scripts/restore_local.ps1 -BackupDir "<path-to-backup-folder>"

## Recovery Objective

- RPO: 24 hours
- RTO: 60 minutes

## Validation Checklist

- Restore completed without file corruption.
- API /health returns status ok.
- /search and /recommend endpoints return valid payload.
