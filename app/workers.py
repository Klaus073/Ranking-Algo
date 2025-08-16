from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional
import logging

from .config import settings
from .queue import queue
from .cache import score_cache
from .repo import Database
from .schemas import EnqueueJob, StudentBundle
from .scoring import compute_scores, _load_ce_scorer  # type: ignore
import pandas as pd


db = Database(settings.database_url)
logger = logging.getLogger("ranking.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_checksum(bundle: StudentBundle) -> str:
    payload = canonical_json(bundle.dict())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def handle_job(job: EnqueueJob) -> None:
    # Debounce per user to coalesce bursts
    if not await queue.set_debounce(job.user_id, settings.debounce_ttl_seconds):
        return

    await db.connect()

    # Supabase-only mode (no DATABASE_URL): fetch via Supabase client and compute, log results, skip writes
    if not settings.database_url:
        # If profile present, use CE mapping directly for highest fidelity with new algo
        if job.profile:
            p = job.profile or {}
            year = int(p.get("current_year") or 0)
            university = p.get("university") or "Non-Target"
            alevel_band = p.get("alevel_band") or "Others"
            gcse_band = p.get("gcse_band") or "Pass"
            grade_band = p.get("uni_grades_band") or ("N/A" if year < 2 else "60-69")
            if year >= 3 and grade_band == "80+":
                grade_band = "73-79"
            awards = int(p.get("awards") or 0)
            certs = int(p.get("certifications") or 0)
            exposure = p.get("industry_exposure") or "None"
            months = int(p.get("months_of_experience") or 0)
            bank_tier = p.get("bank_internship_tier") or "N/A"

            def _years_since(y: int, m: int) -> int:
                today = date.today()
                yrs = today.year - y + (today.month - m) / 12.0
                return int(max(0, round(yrs)))

            internships = []
            for it in p.get("internships") or []:
                try:
                    tier_num = int(str(it.get("tier")).strip())
                except Exception:
                    tier_num = 3
                y = int(it.get("year") or date.today().year)
                m = int(it.get("months") or 0)
                internships.append({"tier": tier_num, "months": m, "years": _years_since(y, 6)})

            def _map_soc_size(s: str) -> int:
                s = (s or "").lower()
                return {"small": 30, "medium": 60, "large": 100}.get(s, 30)

            society = []
            for r in p.get("society_roles") or []:
                role = (r.get("role_title") or "member").strip().lower()
                size = _map_soc_size(r.get("society_size") or "small")
                yrs = int(r.get("years_ago") or 0)
                society.append({"role": role, "size": size, "years": yrs})

            Scorer, CFG = _load_ce_scorer()
            if Scorer is None:
                logger.error("CE scoring module not found; skipping job user_id=%s", job.user_id)
                return
            row = {
                "ID": job.user_id,
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
            df = pd.DataFrame([row])
            scorer = Scorer(CFG)
            out = scorer.score(df)
            rec = out.iloc[0]
            composite = float(rec["Composite"]) 
            academic = float(rec["Academic"]) 
            experience = float(rec["Experience"]) 
            stars = str(rec["Stars"]) 

            # If Supabase env is configured, write via Supabase RPC even in no-DB mode
            if settings.supabase_url and (settings.supabase_service_key or settings.supabase_anon_key):
                L = CFG["lookups"]
                wcfg = CFG["academic_weights"][year]
                v_university = v_grades = v_alevel = v_gcse = 0.0
                if year == 0:
                    v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)
                    v_gcse = (L["gcse"].get(gcse_band, 0) / 2.0)
                elif year == 1:
                    v_university = (L["uni_y1"].get(university, 20) / 30.0)
                    v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)
                    v_gcse = (L["gcse"].get(gcse_band, 0) / 2.0)
                elif year == 2:
                    v_grades = (L["grade_y2"].get(grade_band, 32) / 40.0)
                    v_university = (L["uni_y2"].get(university, 17) / 25.0)
                    v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)
                else:
                    v_grades = (L["grade_y3"].get(grade_band, 36) / 45.0)
                    v_university = (L["uni_y3"].get(university, 10) / 20.0)
                    v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)

                v_awards = (awards / 15.0)
                v_certs = (certs / 10.0)
                components_ac: Dict[str, float] = {}
                if "grades" in wcfg:
                    components_ac["grades"] = wcfg.get("pts", 0) * wcfg.get("grades", 0) * (v_grades or 0.0)
                if "university" in wcfg:
                    components_ac["university"] = wcfg.get("pts", 0) * wcfg.get("university", 0) * (v_university or 0.0)
                if "alevel" in wcfg:
                    components_ac["alevel"] = wcfg.get("pts", 0) * wcfg.get("alevel", 0) * (v_alevel or 0.0)
                if "gcse" in wcfg:
                    components_ac["gcse"] = wcfg.get("pts", 0) * wcfg.get("gcse", 0) * (v_gcse or 0.0)
                if "awards" in wcfg:
                    components_ac["awards"] = wcfg.get("pts", 0) * wcfg.get("awards", 0) * v_awards
                if "certs" in wcfg:
                    components_ac["certs"] = wcfg.get("pts", 0) * wcfg.get("certs", 0) * v_certs

                bank_pts = CFG["lookups"]["bank_tier"].get(bank_tier, 0)
                exposure_pts = CFG["lookups"]["exposure"].get(exposure, 0)
                base_raw = (bank_pts + exposure_pts) * max(0, months) / 3.0
                base = base_raw * CFG["experience_caps"].get(year, 60) / 100.0
                DECAY_RATE, DECAY_FLOOR = 0.10, 0.50
                def decay(yrs: int) -> float:
                    return max(1 - DECAY_RATE * yrs, DECAY_FLOOR)
                INTERN_TIER_PTS = {1: 100, 2: 80, 3: 60}
                if internships:
                    total = 0.0
                    total_months = 0
                    for it in internships:
                        total += INTERN_TIER_PTS.get(it["tier"], 0) * it["months"] * decay(it["years"]) 
                        total_months += it["months"]
                    intern_val = (total / total_months) if total_months else 0.0
                else:
                    intern_val = 0.0
                SOCIETY_ROLE_PTS = {"president": 100, "vice": 80, "committee": 60, "member": 40}
                soc_vals = []
                for r in society:
                    base_r = SOCIETY_ROLE_PTS.get(r["role"], 0)
                    size_factor = (r["size"] + 1) ** 0.5 / 10.0
                    soc_vals.append(base_r * size_factor * decay(r["years"]))
                society_val = sum(soc_vals) / len(soc_vals) if soc_vals else 0.0
                components_ex = {
                    "base": round(base, 4),
                    "internships": round(intern_val, 4),
                    "society": round(society_val, 4),
                }
                eff_w = {k: float(v) for k, v in wcfg.items() if k in {"grades","university","awards","certs","alevel","gcse"}}

                compute_run_id = str(uuid.uuid4())
                checksum = "sha256:" + hashlib.sha256(canonical_json(p).encode("utf-8")).hexdigest()

                params = {
                    "p_user_id": job.user_id,
                    "p_academic": academic,
                    "p_experience": experience,
                    "p_composite": composite,
                    "p_stars": stars,
                    "p_config_version": settings.config_version,
                    "p_compute_run_id": compute_run_id,
                    "p_input_checksum": checksum,
                    "p_academic_components": components_ac,
                    "p_experience_components": components_ex,
                    "p_effective_academic_weights": eff_w,
                }
                # Gate writes on verification; cache otherwise
                try:
                    if await score_cache.is_verified(job.user_id):
                        await db.call_save_compute_result_supabase(params)
                        logger.info(
                            "Saved CE scores via Supabase RPC: user_id=%s composite=%.2f academic=%.2f experience=%.2f stars=%s",
                            job.user_id, composite, academic, experience, stars,
                        )
                        logger.info(
                            "DB payload preview: checksum=%s ac=%s ex=%s eff_w=%s",
                            checksum,
                            json.dumps(components_ac, separators=(",", ":")),
                            json.dumps(components_ex, separators=(",", ":")),
                            json.dumps(eff_w, separators=(",", ":")),
                        )
                    else:
                        await score_cache.set_scores(job.user_id, params)
                        logger.info(
                            "Cached CE scores pending verification: user_id=%s reason=%s composite=%.2f academic=%.2f experience=%.2f stars=%s",
                            job.user_id,
                            job.reason,
                            composite,
                            academic,
                            experience,
                            stars,
                        )
                except Exception:
                    logger.exception("Persist/cache failed for user_id=%s", job.user_id)
            else:
                logger.info(
                    "Computed CE scores (no DB write): user_id=%s composite=%.2f academic=%.2f experience=%.2f index=%.2f stars=%s",
                    job.user_id,
                    composite,
                    academic,
                    experience,
                    float(rec["Index"]),
                    stars,
                )
            return
        # Else, fall back to fetching bundle
        try:
            bundle = await db.fetch_student_bundle(None, job.user_id)  # type: ignore[arg-type]
        except Exception:
            logger.exception("Failed to fetch bundle for user_id=%s via Supabase", job.user_id)
            return

        composite, academic, experience, breakdown = compute_scores(bundle)
        logger.info(
            "Computed scores (no DB write): user_id=%s composite=%.3f academic=%.3f experience=%.3f",
            job.user_id,
            composite,
            academic,
            experience,
        )
        logger.debug("Breakdown: %s", canonical_json(breakdown.dict()))
        return

    # With DATABASE_URL: full transactional path (or Supabase RPC if DB not set but Supabase is)
    async with db.transaction() as conn:
        # Preferred CE path: compute directly from job.profile if present, then call stored procedure
        if job.profile:
            p = job.profile or {}
            year = int(p.get("current_year") or 0)
            university = p.get("university") or "Non-Target"
            alevel_band = p.get("alevel_band") or "Others"
            gcse_band = p.get("gcse_band") or "Pass"
            grade_band = p.get("uni_grades_band") or ("N/A" if year < 2 else "60-69")
            if year >= 3 and grade_band == "80+":
                grade_band = "73-79"
            awards = int(p.get("awards") or 0)
            certs = int(p.get("certifications") or 0)
            exposure = p.get("industry_exposure") or "None"
            months = int(p.get("months_of_experience") or 0)
            bank_tier = p.get("bank_internship_tier") or "N/A"

            def _years_since(y: int, m: int) -> int:
                today = date.today()
                yrs = today.year - y + (today.month - m) / 12.0
                return int(max(0, round(yrs)))

            internships = []
            for it in p.get("internships") or []:
                try:
                    tier_num = int(str(it.get("tier")).strip())
                except Exception:
                    tier_num = 3
                y = int(it.get("year") or date.today().year)
                m = int(it.get("months") or 0)
                internships.append({"tier": tier_num, "months": m, "years": _years_since(y, 6)})

            def _map_soc_size(s: str) -> int:
                s = (s or "").lower()
                return {"small": 30, "medium": 60, "large": 100}.get(s, 30)

            society = []
            for r in p.get("society_roles") or []:
                role = (r.get("role_title") or "member").strip().lower()
                size = _map_soc_size(r.get("society_size") or "small")
                yrs = int(r.get("years_ago") or 0)
                society.append({"role": role, "size": size, "years": yrs})

            Scorer, CFG = _load_ce_scorer()
            if Scorer is None:
                logger.error("CE scoring module not found; skipping DB write for user_id=%s", job.user_id)
                return
            row = {
                "ID": job.user_id,
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
            df = pd.DataFrame([row])
            scorer = Scorer(CFG)
            out = scorer.score(df)
            rec = out.iloc[0]

            composite = float(rec["Composite"])  # CE points
            academic = float(rec["Academic"])   # CE points
            experience = float(rec["Experience"])  # CE points
            stars = str(rec["Stars"])  # band string

            # Build components JSON (approx contributions) and effective weights
            L = CFG["lookups"]
            wcfg = CFG["academic_weights"][year]
            # normalized factors per CE formulas
            v_university = v_grades = v_alevel = v_gcse = 0.0
            if year == 0:
                v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)
                v_gcse = (L["gcse"].get(gcse_band, 0) / 2.0)
            elif year == 1:
                v_university = (L["uni_y1"].get(university, 20) / 30.0)
                v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)
                v_gcse = (L["gcse"].get(gcse_band, 0) / 2.0)
            elif year == 2:
                v_grades = (L["grade_y2"].get(grade_band, 32) / 40.0)
                v_university = (L["uni_y2"].get(university, 17) / 25.0)
                v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)
            else:
                v_grades = (L["grade_y3"].get(grade_band, 36) / 45.0)
                v_university = (L["uni_y3"].get(university, 10) / 20.0)
                v_alevel = (L["alevel"].get(alevel_band, 0) / 30.0)

            v_awards = (awards / 15.0)
            v_certs = (certs / 10.0)
            components_ac: Dict[str, float] = {}
            if "grades" in wcfg:
                components_ac["grades"] = wcfg.get("pts", 0) * wcfg.get("grades", 0) * (v_grades or 0.0)
            if "university" in wcfg:
                components_ac["university"] = wcfg.get("pts", 0) * wcfg.get("university", 0) * (v_university or 0.0)
            if "alevel" in wcfg:
                components_ac["alevel"] = wcfg.get("pts", 0) * wcfg.get("alevel", 0) * (v_alevel or 0.0)
            if "gcse" in wcfg:
                components_ac["gcse"] = wcfg.get("pts", 0) * wcfg.get("gcse", 0) * (v_gcse or 0.0)
            if "awards" in wcfg:
                components_ac["awards"] = wcfg.get("pts", 0) * wcfg.get("awards", 0) * v_awards
            if "certs" in wcfg:
                components_ac["certs"] = wcfg.get("pts", 0) * wcfg.get("certs", 0) * v_certs

            # Experience components
            bank_pts = CFG["lookups"]["bank_tier"].get(bank_tier, 0)
            exposure_pts = CFG["lookups"]["exposure"].get(exposure, 0)
            base_raw = (bank_pts + exposure_pts) * max(0, months) / 3.0
            base = base_raw * CFG["experience_caps"].get(year, 60) / 100.0
            # internship component per CE
            DECAY_RATE, DECAY_FLOOR = 0.10, 0.50
            def decay(yrs: int) -> float:
                return max(1 - DECAY_RATE * yrs, DECAY_FLOOR)
            INTERN_TIER_PTS = {1: 100, 2: 80, 3: 60}
            if internships:
                total = 0.0
                total_months = 0
                for it in internships:
                    total += INTERN_TIER_PTS.get(it["tier"], 0) * it["months"] * decay(it["years"]) 
                    total_months += it["months"]
                intern_val = (total / total_months) if total_months else 0.0
            else:
                intern_val = 0.0
            # society component per CE
            SOCIETY_ROLE_PTS = {"president": 100, "vice": 80, "committee": 60, "member": 40}
            soc_vals = []
            for r in society:
                base_r = SOCIETY_ROLE_PTS.get(r["role"], 0)
                size_factor = (r["size"] + 1) ** 0.5 / 10.0
                soc_vals.append(base_r * size_factor * decay(r["years"]))
            society_val = sum(soc_vals) / len(soc_vals) if soc_vals else 0.0

            components_ex = {
                "base": round(base, 4),
                "internships": round(intern_val, 4),
                "society": round(society_val, 4),
            }

            # Effective academic weights (exclude 'pts')
            eff_w = {k: float(v) for k, v in wcfg.items() if k in {"grades","university","awards","certs","alevel","gcse"}}

            compute_run_id = str(uuid.uuid4())
            checksum = "sha256:" + hashlib.sha256(canonical_json(p).encode("utf-8")).hexdigest()

            # Build common params for cache/verify flush or immediate persist
            params = {
                "p_user_id": job.user_id,
                "p_academic": academic,
                "p_experience": experience,
                "p_composite": composite,
                "p_stars": stars,
                "p_config_version": settings.config_version,
                "p_compute_run_id": compute_run_id,
                "p_input_checksum": checksum,
                "p_academic_components": components_ac,
                "p_experience_components": components_ex,
                "p_effective_academic_weights": eff_w,
            }

            # Gate on verification: cache if not verified, otherwise persist now
            try:
                if not await score_cache.is_verified(job.user_id):
                    await score_cache.set_scores(job.user_id, params)
                    logger.info(
                        "Cached CE scores pending verification (DB mode): user_id=%s reason=%s composite=%.2f academic=%.2f experience=%.2f stars=%s",
                        job.user_id,
                        job.reason,
                        composite,
                        academic,
                        experience,
                        stars,
                    )
                    return
            except Exception:
                logger.exception("Verification/cache check failed; proceeding to persist for user_id=%s", job.user_id)

            # Call stored procedure; it writes to all relevant tables internally
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
                    params["p_user_id"],
                    params["p_academic"],
                    params["p_experience"],
                    params["p_composite"],
                    params["p_stars"],
                    params["p_config_version"],
                    params["p_compute_run_id"],
                    params["p_input_checksum"],
                    json.dumps(params["p_academic_components"]),
                    json.dumps(params["p_experience_components"]),
                    json.dumps(params["p_effective_academic_weights"]),
                )
            else:
                # Supabase RPC path
                try:
                    await db.call_save_compute_result_supabase(params)
                except Exception:
                    logger.exception("Supabase RPC save_compute_result failed for user_id=%s", job.user_id)
                    return
            logger.info(
                "Saved CE scores via stored procedure: user_id=%s composite=%.2f academic=%.2f experience=%.2f stars=%s",
                job.user_id,
                composite,
                academic,
                experience,
                stars,
            )
            logger.info(
                "DB payload preview: checksum=%s ac=%s ex=%s eff_w=%s",
                checksum,
                json.dumps(components_ac, separators=(",", ":")),
                json.dumps(components_ex, separators=(",", ":")),
                json.dumps(eff_w, separators=(",", ":")),
            )
            return

        # Legacy fallback: fetch bundle and use legacy compute/write
        try:
            bundle = await db.fetch_student_bundle(conn, job.user_id)
        except Exception:
            logger.exception("Failed to fetch bundle for user_id=%s", job.user_id)
            return

        checksum = compute_checksum(bundle)

        # Idempotency check
        existing = await db.get_ranking_row(conn, job.user_id)
        if existing and existing["input_checksum"] == checksum:
            logger.info("No-op (idempotent): user_id=%s", job.user_id)
            return

        composite, academic, experience, breakdown = compute_scores(bundle)
        logger.info(
            "Computed scores: user_id=%s composite=%.3f academic=%.3f experience=%.3f",
            job.user_id,
            composite,
            academic,
            experience,
        )
        logger.debug("Breakdown: %s", canonical_json(breakdown.dict()))

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

        # Histogram increment (respect configurable bucket width)
        bucket_id = int(composite // settings.histogram_bucket_width)
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
    await score_cache.connect()
    await db.connect()
    supa_cfg = bool(settings.supabase_url and (settings.supabase_service_key or settings.supabase_anon_key))
    logger.info("Worker started. Redis URL=%s SupabaseConfigured=%s DB=%s", settings.redis_url, supa_cfg, bool(settings.database_url))
    try:
        while True:
            item = await queue.dequeue(timeout=5)
            if item is None:
                continue
            try:
                job = EnqueueJob(**item)
            except Exception:
                logger.exception("Failed to parse job payload: %s", item)
                continue
            try:
                logger.info("Processing job: id=%s user_id=%s reason=%s", job.job_id, job.user_id, job.reason)
                await handle_job(job)
            except Exception:
                # In production, add retry/backoff and DLQ. For now, log and continue.
                logger.exception("Unhandled error while processing job id=%s", job.job_id)
    finally:
        await queue.disconnect()
        await db.disconnect()


