## Ranking Algo Service (Starter)

### What this is
FastAPI service + Redis queue + Postgres (Supabase-managed schema), with workers, cron, and scoring functions.

### Quick start (Docker)
1. Ensure Docker is running
2. From repo root:
   - `docker compose up --build`
3. API at `http://localhost:8000`

### Local dev (Python venv)
```
python -m venv .venv
./.venv/Scripts/activate  # Windows
pip install -r requirements.txt
uvicorn app.app:app --reload --loop auto --http auto
```

Run worker:
```
ENVIRONMENT=local python -m app.run_worker
```

Run cron:
```
ENVIRONMENT=local python -m app.run_cron
```

### Schema
Managed in Supabase. Ensure required tables exist.

### Env vars
- `DATABASE_URL` (asyncpg DSN to Supabase Postgres)
- `REDIS_URL` (redis://)
- `WEBHOOK_HMAC_SECRET`
- `CONFIG_VERSION`
 - `SUPABASE_URL`
 - `SUPABASE_ANON_KEY` or `SUPABASE_SERVICE_KEY` (prefer service key server-side)

### Tests
Install dev deps and run:
```
pip install -r requirements.txt
pytest -q
```


