from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Optional

import logging
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import settings
from .queue import queue
from .cache import score_cache
from .repo import Database
from .schemas import EnqueueJob, RankingResponse, WebhookEvent, ScoreBreakdown, SupabaseWebhook
from .scoring import compute_scores
# HMAC and signature verification disabled for simplified setup
import pandas as pd
from datetime import date
from .scoring import _load_ce_scorer  # type: ignore


logger = logging.getLogger("ranking.app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Ranking Service", version="0.1.0")
db = Database(settings.database_url)


@app.on_event("startup")
async def on_startup() -> None:
    if settings.environment != "test":
        await queue.connect()
        # Connect DB only if configured; otherwise rely on Supabase/preview paths
        if settings.database_url:
            try:
                await db.connect()
            except Exception:
                # Allow startup to proceed for read-only endpoints and webhook enqueue
                pass
        await score_cache.connect()
    logger.info(
        "App startup: env=%s supabase_url=%s",
        settings.environment,
        bool(settings.supabase_url),
    )
    logger.info("Redis URL: %s", settings.redis_url)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if settings.environment != "test":
        await queue.disconnect()
        await db.disconnect()
        await score_cache.disconnect()


@app.post("/webhook")
async def webhook_from_supabase(request: Request):
    # No signature verification (testing mode). Be permissive with payload shape.
    raw = await request.body()
    client_ip = request.client.host if request.client else "?"
    logger.info("HTTP %s %s from %s", request.method, request.url.path, client_ip)

    # Try best-effort JSON parse
    data: Optional[dict] = None
    try:
        data = await request.json()
    except Exception:
        logger.exception("Webhook JSON decode failed: body=%s", raw.decode("utf-8", errors="ignore"))
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    # Log payload
    logger.info("Webhook payload keys: %s", list(data.keys()))
    # Log full payload (truncated) at INFO for visibility during dev
    full_payload = json.dumps(data, default=str)
    logger.info("Webhook payload: %s", (full_payload[:5000] + ("..." if len(full_payload) > 5000 else "")))

    # Extract essentials with fallbacks
    event = data.get("event") or data.get("type") or "unknown"
    user_id = (
        data.get("user_id")
        or (data.get("record", {}) or {}).get("user_id")
        or (data.get("profile", {}) or {}).get("user_id")
    )
    if not user_id:
        logger.warning("Webhook missing user_id; ignoring")
        return JSONResponse({"ok": False, "error": "missing_user_id"}, status_code=400)

    # If this is an email verification event, mark verified and flush cached scores
    if event == "email_verified":
        await score_cache.set_verified(user_id)
        pushed = False
        detail: Optional[str] = None
        cached = await score_cache.get_scores(user_id)
        if cached:
            try:
                if settings.database_url:
                    async with db.transaction() as conn:
                        await conn.fetchval(
                            """
                            SELECT public.save_compute_result(
                              p_user_id := $1::uuid,
                              p_academic := $2::double precision,
                              p_experience := $3::double precision,
                              p_composite := $4::double precision,
                              p_stars := $5::text,
                              p_config_version := $6::text,
                              p_compute_run_id := $7::uuid,
                              p_input_checksum := $8::text,
                              p_academic_components := $9::jsonb,
                              p_experience_components := $10::jsonb,
                              p_effective_academic_weights := $11::jsonb
                            )
                            """,
                            cached.get("p_user_id") or user_id,
                            cached["p_academic"],
                            cached["p_experience"],
                            cached["p_composite"],
                            cached.get("p_stars", ""),
                            cached.get("p_config_version", settings.config_version),
                            cached.get("p_compute_run_id"),
                            cached.get("p_input_checksum"),
                            json.dumps(cached.get("p_academic_components", {})),
                            json.dumps(cached.get("p_experience_components", {})),
                            json.dumps(cached.get("p_effective_academic_weights", {})),
                        )
                else:
                    allowed = {
                        "p_user_id",
                        "p_academic",
                        "p_experience",
                        "p_composite",
                        "p_stars",
                        "p_config_version",
                        "p_compute_run_id",
                        "p_input_checksum",
                        "p_academic_components",
                        "p_experience_components",
                        "p_effective_academic_weights",
                    }
                    payload = {k: v for k, v in cached.items() if k in allowed}
                    await db.call_save_compute_result_supabase(payload)
                pushed = True
            except Exception as exc:
                logger.exception("Verification flush failed for user_id=%s: %s", user_id, exc)
            finally:
                await score_cache.clear_scores(user_id)
        return JSONResponse({"ok": True, "pushed": pushed, "detail": detail})

    reason = "user_created" if event == "user_registered" else "student_updated"
    # Merge input/profile and include top-level email if present so worker can upsert user row
    prof = (data.get("profile") or data.get("input") or {})
    if data.get("email") and isinstance(prof, dict):
        prof = {**prof, "email": data.get("email")}

    job = EnqueueJob(
        job_id=str(uuid.uuid4()),
        user_id=user_id,
        reason=reason,
        event_ids=[],
        enqueued_at=datetime.utcnow(),
        config_version=settings.config_version,
        attempt=1,
        # Accept either 'profile' (legacy) or 'input' (Supabase trigger v2)
        profile=prof,
    )
    logger.info("Enqueue job: id=%s user_id=%s reason=%s", job.job_id, job.user_id, job.reason)

    # Debounce frequent updates for student profile changes
    debounced = False
    should_enqueue = True
    if reason == "student_updated":
        try:
            # Only enqueue if no recent event was processed within TTL
            allowed = await queue.set_debounce(user_id, settings.debounce_ttl_seconds)
            if not allowed:
                debounced = True
                should_enqueue = False
        except Exception:
            # Fail-open on debounce so we don't drop events if Redis is down
            logger.exception("Debounce check failed; proceeding to enqueue")
    elif reason == "user_created":
        try:
            # Separate 5s debounce just for registration bursts
            key = f"registration:{user_id}"
            allowed = await queue.set_named_debounce(key, settings.registration_debounce_ttl_seconds)
            if not allowed:
                debounced = True
                should_enqueue = False
        except Exception:
            logger.exception("Registration debounce check failed; proceeding to enqueue")

    # Try to enqueue; in dev, return 200 even if queue is down so you can iterate
    queued = False
    if should_enqueue:
        try:
            await queue.enqueue(job.dict())
            queued = True
        except Exception as exc:
            queued = False
            logger.exception("Enqueue failed: %s", exc)

    return JSONResponse({"ok": True, "queued": queued, "debounced": debounced})


@app.post("/api/webhook/student-updated")
async def webhook_student_updated(request: Request):
    raw = await request.body()
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
    # Debounce: skip enqueue if a recent event exists
    debounced = False
    try:
        allowed = await queue.set_debounce(event.user_id, settings.debounce_ttl_seconds)
        if allowed:
            await queue.enqueue(job.dict())
            queued = True
        else:
            debounced = True
            queued = False
    except Exception:
        # Fail-open: try enqueue even if debounce check failed
        await queue.enqueue(job.dict())
        queued = True
    return JSONResponse({"status": "ok", "queued": queued, "debounced": debounced})


@app.post("/api/verify/{user_id}")
async def verification_hook(user_id: str):
    # Mark user verified, then attempt to flush cached scores to DB
    await score_cache.set_verified(user_id)
    pushed = False
    detail: Optional[str] = None
    # Try to read cached scores and persist
    cached = await score_cache.get_scores(user_id)
    if cached:
        # Registration-specific debounce: avoid double push right after signup
        try:
            allowed = await queue.set_named_debounce(f"registration-flush:{user_id}", settings.registration_debounce_ttl_seconds)
            if not allowed:
                return {"ok": True, "pushed": False, "detail": "debounced"}
        except Exception:
            # Continue on failure
            pass
        try:
            async with db.transaction() as conn:
                # Support both CE RPC payload and legacy bundle path
                if "p_composite" in cached:
                    # CE path: call stored procedure if DB URL set, otherwise Supabase RPC
                    if settings.database_url:
                        await conn.fetchval(
                            """
                            SELECT public.save_compute_result(
                              p_user_id := $1::uuid,
                              p_academic := $2::double precision,
                              p_experience := $3::double precision,
                              p_composite := $4::double precision,
                              p_stars := $5::text,
                              p_config_version := $6::text,
                              p_compute_run_id := $7::uuid,
                              p_input_checksum := $8::text,
                              p_academic_components := $9::jsonb,
                              p_experience_components := $10::jsonb,
                              p_effective_academic_weights := $11::jsonb
                            )
                            """,
                            cached.get("p_user_id") or user_id,
                            cached["p_academic"],
                            cached["p_experience"],
                            cached["p_composite"],
                            cached.get("p_stars", ""),
                            cached.get("p_config_version", settings.config_version),
                            cached.get("p_compute_run_id"),
                            cached.get("p_input_checksum"),
                            json.dumps(cached.get("p_academic_components", {})),
                            json.dumps(cached.get("p_experience_components", {})),
                            json.dumps(cached.get("p_effective_academic_weights", {})),
                        )
                    else:
                        await db.call_save_compute_result_supabase(cached)
                    pushed = True
                else:
                    # Legacy path not expected in cache; ignore
                    detail = "cached payload missing CE fields"
        finally:
            # Clear cache regardless; we rely on idempotency in DB function
            await score_cache.clear_scores(user_id)
    return {"ok": True, "pushed": pushed, "detail": detail}


@app.get("/api/ranking/{user_id}", response_model=RankingResponse)
async def get_ranking(user_id: str):
    # This endpoint requires direct DB for histogram and breakdown; enforce config
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
        # With CE algo, composite is not normalized; histogram bucket width 5 still applies
        bucket = int(row["composite"] // settings.histogram_bucket_width)
        below = sum(h["count"] for h in hist if h["bucket_id"] < bucket)
        inside = next((h["count"] for h in hist if h["bucket_id"] == bucket), 0)
        frac = (row["composite"] - bucket * settings.histogram_bucket_width) / float(settings.histogram_bucket_width)
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
        if not b:
            raise HTTPException(status_code=404, detail="breakdown not found")

        breakdown = ScoreBreakdown(
            academic_components=b["academic_components"],
            experience_components=b["experience_components"],
            effective_academic_weights=b["effective_academic_weights"],
            academic_total=b["academic_total"],
            experience_total=b["experience_total"],
            composite=b["composite"],
        )

        return RankingResponse(
            user_id=row["user_id"],
            composite=row["composite"],
            academic=row["academic"],
            experience=row["experience"],
            percentile=percentile,
            rank=row["rank"],
            out_of=row["out_of"],
            breakdown=breakdown,
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



def _years_since(year: int, month: int) -> int:
    today = date.today()
    years = today.year - year + (today.month - month) / 12.0
    return int(max(0, round(years)))


def _map_society_size(text: str) -> int:
    t = (text or "").strip().lower()
    return {"small": 30, "medium": 60, "large": 100}.get(t, 30)


def _map_bank_tier(text: str | None) -> str:
    t = (text or "").strip()
    allow = {"Bulge", "Elite", "Mid", "UpperMid", "LowerMid", "Boutique", "N/A"}
    return t if t in allow else "N/A"


@app.post("/api/score/preview")
async def score_preview(payload: dict):
    data = payload.get("input") or payload

    year = int(data.get("current_year") or 0)
    university = data.get("university") or "Non-Target"
    alevel_band = data.get("alevel_band") or "Others"
    gcse_band = data.get("gcse_band") or "Pass"
    grade_band = data.get("uni_grades_band") or ("N/A" if year < 2 else "60-69")
    # Normalize grade band to CE lookups per year
    if year >= 3 and grade_band == "80+":
        grade_band = "73-79"
    awards = int(data.get("awards") or 0)
    certs = int(data.get("certifications") or 0)
    exposure = data.get("industry_exposure") or "None"
    months = int(data.get("months_of_experience") or 0)
    bank_tier = _map_bank_tier(data.get("bank_internship_tier"))

    internships_in = data.get("internships") or []
    internships = []
    for it in internships_in:
        try:
            tier_num = int(str(it.get("tier")).strip())
        except Exception:
            tier_num = 3
        y = int(it.get("year") or date.today().year)
        m = int(it.get("months") or 0)
        internships.append({"tier": tier_num, "months": m, "years": _years_since(y, 6)})

    society_in = data.get("society_roles") or []
    society = []
    for r in society_in:
        role = (r.get("role_title") or "member").strip().lower()
        size = _map_society_size(r.get("society_size") or "small")
        yrs = int(r.get("years_ago") or 0)
        society.append({"role": role, "size": size, "years": yrs})

    # Build CE row
    row = {
        "ID": payload.get("user_id") or "preview",
        "year": year,
        "university": university,
        "alevel": alevel_band,
        "gcse": gcse_band,
        "grades": grade_band,
        "awards": awards,
        "certs": certs,
        "bank_tier": bank_tier,
        "exposure": exposure,
        "months": months,
        "internships": internships,
        "society": society,
    }

    Scorer, CFG = _load_ce_scorer()
    if Scorer is None:
        raise HTTPException(status_code=500, detail="CE scoring module not found")
    scorer = Scorer(CFG)
    df = pd.DataFrame([row])
    out = scorer.score(df)
    rec = out.iloc[0]
    result = {
        "user_id": row["ID"],
        "year": row["year"],
        "academic": float(rec["Academic"]),
        "experience": float(rec["Experience"]),
        "composite": float(rec["Composite"]),
        "index": float(rec["Index"]),
        "stars": rec["Stars"],
        "row": row,
    }
    return result


