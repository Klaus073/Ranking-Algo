from __future__ import annotations

import json
from typing import Any, Dict, Optional
from datetime import datetime, date

import redis.asyncio as redis

from .config import settings


class RedisQueue:
    def __init__(self, url: str, queue_key: str = "ranking_jobs") -> None:
        self._url = url
        self._queue_key = queue_key
        self._redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = redis.from_url(self._url, encoding="utf-8", decode_responses=True)

    async def disconnect(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def enqueue(self, payload: Dict[str, Any]) -> None:
        assert self._redis is not None
        def _default(obj: Any):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Type not serializable: {type(obj)}")
        await self._redis.rpush(self._queue_key, json.dumps(payload, default=_default))

    async def dequeue(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        assert self._redis is not None
        item = await self._redis.blpop(self._queue_key, timeout=timeout)
        if item is None:
            return None
        _, data = item
        return json.loads(data)

    async def set_debounce(self, user_id: str, ttl_seconds: int) -> bool:
        assert self._redis is not None
        key = f"debounce:{user_id}"
        # SETNX with expiration: use set with ex and nx
        was_set = await self._redis.set(key, "1", ex=ttl_seconds, nx=True)
        return bool(was_set)

    async def acquire_lock(self, name: str, ttl_seconds: int = 60) -> bool:
        assert self._redis is not None
        key = f"lock:{name}"
        return bool(await self._redis.set(key, "1", ex=ttl_seconds, nx=True))

    async def release_lock(self, name: str) -> None:
        assert self._redis is not None
        await self._redis.delete(f"lock:{name}")


queue = RedisQueue(settings.redis_url)


