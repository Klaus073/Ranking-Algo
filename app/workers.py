from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .config import settings
from .queue import queue
from .repo import Database
from .schemas import EnqueueJob, StudentBundle
from .scoring import compute_scores


db = Database(settings.database_url)


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_checksum(bundle: StudentBundle) -> str:
    payload = canonical_json(bundle.dict())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def handle_job(job: EnqueueJob) -> None:
    # Debounce per user to coalesce bursts
    if not await queue.set_debounce(job.user_id, settings.debounce_ttl_seconds):
        return

    # TODO: Fetch bundle from DB (Supabase service role). For now, assume bundle is provided externally or stub.
    # Replace the following stub with real fetching logic.
    bundle = StudentBundle(
        user_id=job.user_id,
        academic_year=2,
        university_tier="Other",
        grade=None,
        alevels=[],
        num_gcse=0,
        awards_count=0,
        internships=[],
        total_months_experience=0,
        society_roles=[],
        certifications_count=0,
        exposure="None",
    )

    checksum = compute_checksum(bundle)

    await db.connect()
    async with db.transaction() as conn:
        # Idempotency check
        existing = await db.get_ranking_row(conn, job.user_id)
        if existing and existing["input_checksum"] == checksum:
            return

        composite, academic, experience, breakdown = compute_scores(bundle)
        compute_run_id = str(uuid.uuid4())

        old_score = existing["composite"] if existing else None
        await db.upsert_student_results(
            conn,
            user_id=job.user_id,
            composite=composite,
            academic=academic,
            experience=experience,
            breakdown=breakdown,
            config_version=settings.config_version,
            compute_run_id=compute_run_id,
            input_checksum=checksum,
        )

        # Histogram increment
        bucket_id = int(composite // 5)
        await db.upsert_histogram_increment(conn, bucket_id, 1)

        # Audit log
        await db.insert_update_log(
            conn,
            user_id=job.user_id,
            reason=job.reason,
            old_score=old_score,
            new_score=composite,
            payload=job.dict(),
            config_version=settings.config_version,
            compute_run_id=compute_run_id,
        )


async def worker_loop() -> None:
    await queue.connect()
    await db.connect()
    try:
        while True:
            item = await queue.dequeue(timeout=5)
            if item is None:
                continue
            try:
                job = EnqueueJob(**item)
            except Exception:
                continue
            try:
                await handle_job(job)
            except Exception:
                # In production, add retry/backoff and DLQ. For now, swallow to keep loop running.
                pass
    finally:
        await queue.disconnect()
        await db.disconnect()


