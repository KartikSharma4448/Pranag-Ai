# Secrets Inventory

Updated: 2026-04-07

| Secret Name | Purpose | Rotation Period | Last Rotated | Owner | Status |
|---|---|---|---|---|---|
| IMD_API_KEY | IMD climate API auth | 90 days | pending | Data Owner | pending official issue |
| API_ACCESS_KEY | FastAPI access control | 30 days | 2026-04-07 | Platform Owner | active |
| REDIS_URL | Cache backend connectivity | 180 days | 2026-04-07 | Platform Owner | active |
| OBJECT_STORAGE_ACCESS_KEY_ID | Bucket write credential | 90 days | 2026-04-07 | Platform Owner | active |
| OBJECT_STORAGE_SECRET_ACCESS_KEY | Bucket write credential | 90 days | 2026-04-07 | Platform Owner | active |
| MP_API_KEY | Materials source API | 90 days | pending | Data Owner | optional/fallback available |
| NCBI_API_KEY | Biology source API | 90 days | pending | Data Owner | optional/fallback available |

## Secret Handling Policy

- Never commit real secrets to git.
- Use .env only for local development.
- Use cloud secret manager for production.
- Rotate immediately on leakage suspicion.
