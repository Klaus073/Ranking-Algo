from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Optional

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import settings
from .queue import queue
from .repo import Database
from .schemas import EnqueueJob, RankingResponse, WebhookEvent, ScoreBreakdown
from .scoring import compute_scores
from .security import verify_hmac_headers


app = FastAPI(title="Ranking Service", version="0.1.0")
db = Database(settings.database_url)


@app.on_event("startup")
async def on_startup() -> None:
    await queue.connect()
    await db.connect()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await queue.disconnect()
    await db.disconnect()


@app.post("/api/webhook/user-created")
async def webhook_user_created(
    request: Request,
    x_timestamp: str = Header(..., alias="X-Timestamp"),
    x_signature: str = Header(..., alias="X-Signature"),
):
    raw = await request.body()
    verify_hmac_headers(raw, x_timestamp, x_signature)

    try:
        event = WebhookEvent.parse_raw(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload")

    job = EnqueueJob(
        job_id=str(uuid.uuid4()),
        user_id=event.user_id,
        reason="user_created",
        event_ids=[event.event_id],
        enqueued_at=datetime.utcnow(),
        config_version=settings.config_version,
        attempt=1,
    )
    await queue.enqueue(job.dict())
    return JSONResponse({"status": "ok"})


@app.post("/api/webhook/student-updated")
async def webhook_student_updated(
    request: Request,
    x_timestamp: str = Header(..., alias="X-Timestamp"),
    x_signature: str = Header(..., alias="X-Signature"),
):
    raw = await request.body()
    verify_hmac_headers(raw, x_timestamp, x_signature)

    try:
        event = WebhookEvent.parse_raw(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload")

    job = EnqueueJob(
        job_id=str(uuid.uuid4()),
        user_id=event.user_id,
        reason="student_updated",
        event_ids=[event.event_id],
        enqueued_at=datetime.utcnow(),
        config_version=settings.config_version,
        attempt=1,
    )
    await queue.enqueue(job.dict())
    return JSONResponse({"status": "ok"})


@app.get("/api/ranking/{user_id}", response_model=RankingResponse)
async def get_ranking(user_id: str):
    async with db.transaction() as conn:
        row = await conn.fetchrow(
            """
            SELECT r.user_id, r.composite, r.academic, r.experience, r.rank,
                   gs.total_users AS out_of
            FROM student_rankings r
            LEFT JOIN global_ranking_stats gs ON gs.id = 1
            WHERE r.user_id = $1
            """,
            user_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="user not found")

        # Compute approx percentile from histogram
        hist = await db.fetch_histogram(conn)
        total = sum(h["count"] for h in hist) if hist else 0
        bucket = int(row["composite"] // 5)
        below = sum(h["count"] for h in hist if h["bucket_id"] < bucket)
        inside = next((h["count"] for h in hist if h["bucket_id"] == bucket), 0)
        frac = (row["composite"] - bucket * 5) / 5.0
        approx_below = below + frac * inside
        percentile = 100.0 * approx_below / max(1, total - 1)

        # fetch breakdown
        b = await conn.fetchrow(
            """
            SELECT academic_components, experience_components, effective_academic_weights,
                   academic_total, experience_total, composite
            FROM student_score_breakdown
            WHERE user_id = $1
            """,
            user_id,
        )
        breakdown = ScoreBreakdown(
            academic_components=b["academic_components"],
            experience_components=b["experience_components"],
            effective_academic_weights=b["effective_academic_weights"],
            academic_total=b["academic_total"],
            experience_total=b["experience_total"],
            composite=b["composite"],
        ) if b else None

        return RankingResponse(
            user_id=row["user_id"],
            composite=row["composite"],
            academic=row["academic"],
            experience=row["experience"],
            percentile=percentile,
            rank=row["rank"],
            out_of=row["out_of"],
            breakdown=breakdown,  # type: ignore[arg-type]
        )


@app.get("/api/config")
async def get_config():
    return {"config_version": settings.config_version}


@app.post("/api/ranking/recalculate/{user_id}")
async def recalc(user_id: str):
    job = EnqueueJob(
        job_id=str(uuid.uuid4()),
        user_id=user_id,
        reason="manual",
        event_ids=[],
        enqueued_at=datetime.utcnow(),
        config_version=settings.config_version,
    )
    await queue.enqueue(job.dict())
    return {"status": "enqueued"}


