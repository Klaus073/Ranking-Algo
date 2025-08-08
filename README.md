## Ranking Algo Service (Starter)

### What this is
FastAPI service + Redis queue + Postgres, with workers, cron, scoring functions, and migrations per the spec.

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
python -m app.run_worker
```

Run cron:
```
python -m app.run_cron
```

### Migrations
```
alembic upgrade head
```

### Env vars
- `DATABASE_URL` (asyncpg DSN)
- `REDIS_URL` (redis://)
- `WEBHOOK_HMAC_SECRET`
- `CONFIG_VERSION`


