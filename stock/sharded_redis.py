"""
Application-level Redis sharding.

ShardedRedis wraps N Redis clients and routes operations to the correct shard
using modulo hashing on the entity ID. When N=1, behaviour is identical to a
single Redis client.
"""

import asyncio
import hashlib
from collections import defaultdict
from typing import AsyncIterator

from redis.asyncio import Redis


def _stable_hash(value: str) -> int:
    """Deterministic hash that is stable across processes and restarts."""
    return int(hashlib.sha256(value.encode()).hexdigest(), 16)


class ShardedRedis:
    def __init__(self, shards: list[Redis]):
        if not shards:
            raise ValueError("At least one Redis shard is required")
        self.shards = shards
        self.n = len(shards)

    def shard_for(self, entity_id: str) -> Redis:
        """Route to shard by entity_id. Works for integer strings and UUIDs."""
        return self.shards[self.shard_index_for(entity_id)]

    def shard_index_for(self, entity_id: str) -> int:
        """Return the shard index for an entity_id."""
        try:
            return int(entity_id) % self.n
        except (ValueError, TypeError):
            return _stable_hash(entity_id) % self.n

    def primary(self) -> Redis:
        """Shard 0 — used for infrastructure keys (leases, etc.)."""
        return self.shards[0]

    async def mset_sharded(self, kv_pairs: dict[str, bytes]):
        """Split an MSET across shards based on key."""
        buckets: dict[int, dict[str, bytes]] = defaultdict(dict)
        for key, val in kv_pairs.items():
            idx = self.shard_index_for(key)
            buckets[idx][key] = val
        await asyncio.gather(*(self.shards[i].mset(b) for i, b in buckets.items()))

    async def scan_all(self, match: str) -> AsyncIterator[tuple[bytes, Redis]]:
        """Scan across all shards, yielding (raw_key, shard) pairs."""
        for shard in self.shards:
            async for key in shard.scan_iter(match=match):
                yield key, shard

    async def get_from_any(self, key: str) -> tuple[bytes | None, Redis | None]:
        """Broadcast GET across all shards, return first hit and its shard."""
        results = await asyncio.gather(*(shard.get(key) for shard in self.shards))
        for shard, result in zip(self.shards, results):
            if result is not None:
                return result, shard
        return None, None

    def register_script_on_all(self, lua_code: str) -> list:
        """Register a Lua script on every shard, returning list of script objects."""
        return [shard.register_script(lua_code) for shard in self.shards]

    async def close_all(self):
        """Close all shard connections."""
        for shard in self.shards:
            await shard.aclose()
