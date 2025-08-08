from __future__ import annotations

from typing import Optional

from .queue import queue
from .repo import Database
from .config import settings


db = Database(settings.database_url)


RANK_SQL = """
WITH ordered AS (
  SELECT user_id, composite, experience, updated_at,
         ROW_NUMBER() OVER (ORDER BY composite DESC, experience DESC, updated_at DESC, user_id ASC) AS r
  FROM student_rankings
)
UPDATE student_rankings s
SET rank = o.r
FROM ordered o
WHERE s.user_id = o.user_id;
"""


GLOBAL_STATS_SQL = """
INSERT INTO global_ranking_stats (id, total_users, p50, p90, p99, updated_at, config_version)
SELECT 1,
       COUNT(*),
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY composite),
       PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY composite),
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY composite),
       NOW(), $1
FROM student_rankings
ON CONFLICT (id) DO UPDATE SET
  total_users = EXCLUDED.total_users,
  p50 = EXCLUDED.p50,
  p90 = EXCLUDED.p90,
  p99 = EXCLUDED.p99,
  updated_at = EXCLUDED.updated_at,
  config_version = EXCLUDED.config_version;
"""


REBUILD_HISTOGRAM_SQL = """
TRUNCATE score_histogram;
WITH buckets AS (
  SELECT FLOOR(composite/5.0)::int AS bucket_id, COUNT(*) AS cnt
  FROM student_rankings
  GROUP BY 1
)
INSERT INTO score_histogram (bucket_id, count)
SELECT bucket_id, cnt FROM buckets;
"""


async def run_cron_once() -> None:
    await queue.connect()
    await db.connect()
    lock_name = "cron_singleflight"
    if not await queue.acquire_lock(lock_name, ttl_seconds=300):
        return
    try:
        async with db.transaction() as conn:
            await conn.execute(RANK_SQL)
            await conn.execute(GLOBAL_STATS_SQL, settings.config_version)
            await conn.execute(REBUILD_HISTOGRAM_SQL)
    finally:
        await queue.release_lock(lock_name)


