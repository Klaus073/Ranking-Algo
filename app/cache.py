from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

import redis.asyncio as redis

from .config import settings


class RedisScoreCache:
    def __init__(self, url: str) -> None:
        self._url = url
        self._redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = redis.from_url(self._url, encoding="utf-8", decode_responses=True)

    async def disconnect(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def set_scores(self, user_id: str, payload: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        assert self._redis is not None
        key = f"scores:{user_id}"
        await self._redis.set(key, json.dumps(payload), ex=ttl_seconds or settings.score_cache_ttl_seconds)

    async def get_scores(self, user_id: str) -> Optional[Dict[str, Any]]:
        assert self._redis is not None
        key = f"scores:{user_id}"
        raw = await self._redis.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    async def clear_scores(self, user_id: str) -> None:
        assert self._redis is not None
        await self._redis.delete(f"scores:{user_id}")

    async def set_verified(self, user_id: str) -> None:
        assert self._redis is not None
        key = f"verified:{user_id}"
        # Persist indefinitely; can be cleared via clear_verified if needed
        await self._redis.set(key, "1")

    async def is_verified(self, user_id: str) -> bool:
        assert self._redis is not None
        key = f"verified:{user_id}"
        return bool(await self._redis.get(key))

    async def clear_verified(self, user_id: str) -> None:
        assert self._redis is not None
        await self._redis.delete(f"verified:{user_id}")


score_cache = RedisScoreCache(settings.redis_url)



