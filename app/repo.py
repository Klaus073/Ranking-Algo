from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .config import settings
from .schemas import (
    ALevel,
    Internship,
    ScoreBreakdown,
    SocietyRole,
    StudentBundle,
)


class Database:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=20)

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self):
        assert self._pool is not None, "Database not connected"
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def fetch_histogram(self, conn: asyncpg.Connection) -> List[asyncpg.Record]:
        return await conn.fetch("SELECT bucket_id, count FROM score_histogram ORDER BY bucket_id ASC")

    async def upsert_histogram_increment(self, conn: asyncpg.Connection, bucket_id: int, delta: int = 1) -> None:
        await conn.execute(
            """
            INSERT INTO score_histogram (bucket_id, count) VALUES ($1, $2)
            ON CONFLICT (bucket_id) DO UPDATE SET count = score_histogram.count + EXCLUDED.count
            """,
            bucket_id,
            delta,
        )

    async def get_ranking_row(self, conn: asyncpg.Connection, user_id: str) -> Optional[asyncpg.Record]:
        return await conn.fetchrow(
            """
            SELECT user_id, composite, academic, experience, rank, percentile, updated_at, input_checksum
            FROM student_rankings WHERE user_id = $1
            """,
            user_id,
        )

    async def upsert_student_results(
        self,
        conn: asyncpg.Connection,
        *,
        user_id: str,
        composite: float,
        academic: float,
        experience: float,
        breakdown: ScoreBreakdown,
        config_version: str,
        compute_run_id: str,
        input_checksum: str,
    ) -> None:
        # Upsert current ranking
        await conn.execute(
            """
            INSERT INTO student_rankings (user_id, composite, academic, experience, updated_at, config_version, compute_run_id, input_checksum)
            VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7)
            ON CONFLICT (user_id) DO UPDATE SET
              composite = EXCLUDED.composite,
              academic = EXCLUDED.academic,
              experience = EXCLUDED.experience,
              updated_at = EXCLUDED.updated_at,
              config_version = EXCLUDED.config_version,
              compute_run_id = EXCLUDED.compute_run_id,
              input_checksum = EXCLUDED.input_checksum
            """,
            user_id,
            composite,
            academic,
            experience,
            config_version,
            compute_run_id,
            input_checksum,
        )

        # Insert history
        await conn.execute(
            """
            INSERT INTO student_score_history (user_id, computed_at, composite, academic, experience, config_version, compute_run_id)
            VALUES ($1, NOW(), $2, $3, $4, $5, $6)
            """,
            user_id,
            composite,
            academic,
            experience,
            config_version,
            compute_run_id,
        )

        # Upsert breakdown
        await conn.execute(
            """
            INSERT INTO student_score_breakdown (user_id, academic_components, experience_components, effective_academic_weights, academic_total, experience_total, composite, updated_at, config_version, compute_run_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $9)
            ON CONFLICT (user_id) DO UPDATE SET
              academic_components = EXCLUDED.academic_components,
              experience_components = EXCLUDED.experience_components,
              effective_academic_weights = EXCLUDED.effective_academic_weights,
              academic_total = EXCLUDED.academic_total,
              experience_total = EXCLUDED.experience_total,
              composite = EXCLUDED.composite,
              updated_at = EXCLUDED.updated_at,
              config_version = EXCLUDED.config_version,
              compute_run_id = EXCLUDED.compute_run_id
            """,
            user_id,
            json.dumps(breakdown.academic_components.dict()),
            json.dumps(breakdown.experience_components.dict()),
            json.dumps(breakdown.effective_academic_weights),
            breakdown.academic_total,
            breakdown.experience_total,
            breakdown.composite,
            config_version,
            compute_run_id,
        )

    async def insert_update_log(
        self,
        conn: asyncpg.Connection,
        *,
        user_id: str,
        reason: str,
        old_score: Optional[float],
        new_score: float,
        payload: Dict[str, Any],
        config_version: str,
        compute_run_id: str,
    ) -> None:
        delta = None if old_score is None else new_score - old_score
        await conn.execute(
            """
            INSERT INTO ranking_updates_log (user_id, reason, old_score, new_score, delta, payload, created_at, config_version, compute_run_id)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW(), $7, $8)
            """,
            user_id,
            reason,
            old_score,
            new_score,
            delta,
            json.dumps(payload),
            config_version,
            compute_run_id,
        )

    # -----------------------------
    # Fetch bundle (Supabase schema)
    # -----------------------------

    @staticmethod
    def _classify_university_tier(university: Optional[str]) -> str:
        if not university:
            return "Other"
        u = university.lower().strip()
        if "oxford" in u or "cambridge" in u:
            return "Oxbridge"
        if "imperial" in u or u in {"lse", "london school of economics"}:
            return "Imperial/LSE"
        if u in {"ucl", "university college london"}:
            return "UCL"
        if "edinburgh" in u or u in {"kcl", "king's college london", "kings college london"}:
            return "KCL/Edinburgh"
        if any(k in u for k in ["warwick", "bath", "durham"]):
            return "Warwick/Bath/Durham"
        return "Other"

    @staticmethod
    def _normalize_grade(grade_text: Optional[str]) -> Optional[str]:
        if not grade_text:
            return None
        g = grade_text.strip().lower()
        if g.startswith("first") or g == "1st":
            return "First"
        if "2:1" in g or "2-1" in g or g == "upper second":
            return "2:1"
        if "2:2" in g or "2-2" in g or g == "lower second":
            return "2:2"
        if g.startswith("third") or g == "3rd":
            return "Third"
        return None

    @staticmethod
    def _normalize_exposure(exposure_text: Optional[str]) -> str:
        if not exposure_text:
            return "None"
        e = exposure_text.strip().lower()
        if "placement" in e:
            return "Placement"
        if "summer" in e:
            return "Summer Internship"
        if "spring" in e:
            return "Spring Week"
        if "shadow" in e:
            return "Shadowing"
        return "None"

    @staticmethod
    def _normalize_intern_tier(tier_text: Optional[str]) -> Optional[str]:
        if not tier_text:
            return None
        t = tier_text.strip().lower()
        if "bulge" in t:
            return "Bulge Bracket"
        if "elite" in t:
            return "Elite Boutique"
        if "middle" in t:
            return "Middle Market"
        if "regional" in t or "local" in t:
            return "Regional"
        return None

    @staticmethod
    def _normalize_society_size(size_text: str) -> str:
        s = size_text.strip().lower()
        if s == "large":
            return "Large"
        if s == "medium":
            return "Medium"
        return "Small"

    @staticmethod
    def _normalize_role_title(title: str) -> str:
        t = title.strip().lower()
        if "president" in t or "chair" in t:
            return "President"
        if any(k in t for k in ["committee", "treasurer", "secretary", "vp", "vice"]):
            return "Committee"
        return "Member"

    async def fetch_student_bundle(self, conn: asyncpg.Connection, user_id: str) -> StudentBundle:
        # Profiles row (main)
        prof = await conn.fetchrow(
            """
            SELECT user_id, current_year, university, grades, industry_exposure,
                   months_of_experience, awards, certifications
            FROM student_profiles WHERE user_id = $1
            """,
            user_id,
        )

        academic_year = int(prof["current_year"]) if prof and prof["current_year"] is not None else 0
        university_tier = self._classify_university_tier(prof["university"] if prof else None)
        grade = self._normalize_grade(prof["grades"] if prof else None)
        total_months = int(prof["months_of_experience"]) if prof and prof["months_of_experience"] is not None else 0
        awards = int(prof["awards"]) if prof and prof["awards"] is not None else 0
        certs = int(prof["certifications"]) if prof and prof["certifications"] is not None else 0
        exposure = self._normalize_exposure(prof["industry_exposure"] if prof else None)

        # GCSEs count
        num_gcse_row = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM student_gcses WHERE user_id = $1",
            user_id,
        )
        num_gcse = int(num_gcse_row["c"]) if num_gcse_row else 0

        # A-levels: schema shows only subject; without grade/category, we leave empty
        # Optionally, derive categories from subject for future refinement.
        alevels: List[ALevel] = []

        # Internships
        intern_rows = await conn.fetch(
            """
            SELECT tier, months, year
            FROM student_internships WHERE user_id = $1
            """,
            user_id,
        )
        internships: List[Internship] = []
        for r in intern_rows:
            tier_norm = self._normalize_intern_tier(r["tier"]) or "Regional"
            months = int(r["months"]) if r["months"] is not None else 0
            year = int(r["year"]) if r["year"] is not None else datetime.utcnow().year
            internships.append(
                Internship(tier=tier_norm, months=months, end_year=year, end_month=6)
            )

        # Society roles
        role_rows = await conn.fetch(
            """
            SELECT role_title, society_size, years_active
            FROM student_society_roles WHERE user_id = $1
            """,
            user_id,
        )
        society_roles: List[SocietyRole] = []
        for r in role_rows:
            role = self._normalize_role_title(r["role_title"]) if r["role_title"] else "Member"
            size = self._normalize_society_size(r["society_size"]) if r["society_size"] else "Small"
            years = int(r["years_active"]) if r["years_active"] is not None else 1
            society_roles.append(SocietyRole(role=role, size=size, years=years))

        return StudentBundle(
            user_id=user_id,
            academic_year=academic_year,
            university_tier=university_tier,
            grade=grade,
            alevels=alevels,
            num_gcse=num_gcse,
            awards_count=awards,
            internships=internships,
            total_months_experience=total_months,
            society_roles=society_roles,
            certifications_count=certs,
            exposure=exposure,
        )


