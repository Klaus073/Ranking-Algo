from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .config import settings
from .schemas import ScoreBreakdown


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


