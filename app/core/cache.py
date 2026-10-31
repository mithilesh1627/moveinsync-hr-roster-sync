"""Master Reference Data Cache with TTL & Eviction

Caches tenant office codes and geofence references with bounded LRU eviction and TTL.
Prevents N+1 database roundtrips during high-throughput event validation and reconciliation.
Staleness bounds: TTL is 300 seconds; local invalidation occurs immediately on office updates.
"""

import time
import threading
from typing import Set, Optional, Dict, Tuple, Any
from collections import OrderedDict

from app.core.config import settings


class BoundedTTLCache:
    """Thread-safe bounded in-memory LRU cache with monotonic TTL expiration."""

    def __init__(self, max_entries: int = 1000, default_ttl_seconds: float = 300.0):
        self._max_entries = max_entries
        self._default_ttl = default_ttl_seconds
        self._cache: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            now = time.monotonic()
            if key not in self._cache:
                return None
            value, expiry = self._cache[key]
            if now > expiry:
                del self._cache[key]
                return None
            # Move to end (MRU)
            self._cache.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        with self._lock:
            now = time.monotonic()
            expiry = now + (ttl_seconds if ttl_seconds is not None else self._default_ttl)
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = (value, expiry)
            if len(self._cache) > self._max_entries:
                self._cache.popitem(last=False)  # Evict least recently used (LRU)

    def invalidate(self, key: str) -> None:
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)


# Global cache instance for office codes: tenant_id -> Set[str] of valid uppercase office codes
office_cache = BoundedTTLCache(
    max_entries=settings.cache_max_entries,
    default_ttl_seconds=settings.cache_ttl_seconds,
)
