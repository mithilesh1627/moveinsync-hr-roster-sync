"""P2 Reference Data Cache Deep Correctness & Concurrency Tests

Verifies requirements from Section 11:
1. BoundedTTLCache monotonic TTL expiration.
2. LRU eviction when capacity reaches max_entries.
3. Cache hit / miss semantics and MRU order updates.
4. Concurrent access safety under multi-threaded load.
5. Invalidation on office creation/updates preventing stale lookups.
"""

import time
import uuid
import threading
import concurrent.futures
import pytest
from app.core.cache import BoundedTTLCache, office_cache
from app.core.database import async_session_factory, sync_engine
from app.repositories.office_repo import OfficeRepository


def test_cache_ttl_expiration():
    """Cache returns value within TTL and returns None after TTL expires."""
    cache = BoundedTTLCache(max_entries=10, default_ttl_seconds=0.05)
    cache.set("key1", "val1")

    # Immediate get
    assert cache.get("key1") == "val1"

    # Wait for TTL to elapse
    time.sleep(0.06)
    assert cache.get("key1") is None


def test_cache_lru_eviction():
    """Cache enforces max_entries bound by evicting the least recently used item."""
    cache = BoundedTTLCache(max_entries=3, default_ttl_seconds=60.0)

    cache.set("k1", "v1")
    cache.set("k2", "v2")
    cache.set("k3", "v3")

    assert len(cache) == 3

    # Access k1 so k2 becomes the oldest / LRU
    assert cache.get("k1") == "v1"

    # Insert k4 -> should evict k2 (LRU)
    cache.set("k4", "v4")
    assert len(cache) == 3

    assert cache.get("k1") == "v1"
    assert cache.get("k2") is None  # Evicted!
    assert cache.get("k3") == "v3"
    assert cache.get("k4") == "v4"


def test_cache_explicit_invalidation_and_clear():
    """Invalidate removes a specific key; clear removes all entries."""
    cache = BoundedTTLCache(max_entries=10, default_ttl_seconds=60.0)
    cache.set("a", 1)
    cache.set("b", 2)

    cache.invalidate("a")
    assert cache.get("a") is None
    assert cache.get("b") == 2

    cache.clear()
    assert len(cache) == 0
    assert cache.get("b") is None


def test_cache_multithreaded_concurrency():
    """Multiple threads concurrently setting and reading keys do not corrupt cache state."""
    cache = BoundedTTLCache(max_entries=50, default_ttl_seconds=5.0)
    num_threads = 10
    iterations = 200

    def worker_task(thread_id: int):
        for i in range(iterations):
            key = f"key-{thread_id}-{i % 20}"
            cache.set(key, f"val-{thread_id}-{i}")
            val = cache.get(key)
            if val is not None:
                assert val.startswith(f"val-{thread_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_task, tid) for tid in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()  # Re-raises any exception

    # Cache length must never exceed max_entries
    assert len(cache) <= 50


@pytest.mark.asyncio
async def test_office_repository_cache_and_invalidation():
    """OfficeRepository caches office codes and invalidates cache when a new office is registered."""
    tenant_id = f"tenant-cache-{uuid.uuid4().hex[:8]}"
    office_code = f"OFF-{uuid.uuid4().hex[:6].upper()}"

    # Clear global office cache for isolation
    office_cache.clear()

    async with async_session_factory() as session:
        repo = OfficeRepository(session)

        # 1. First fetch: DB query (returns empty set)
        codes1 = await repo.get_valid_office_codes(tenant_id)
        assert office_code not in codes1

        # Now tenant-cache-test is cached in office_cache
        assert office_cache.get(tenant_id) == set()

        # 2. Create new office -> automatically invalidates office_cache for this tenant
        new_office = await repo.create_office(
            tenant_id=tenant_id,
            office_code=office_code,
            office_name="Cache Test Campus",
        )
        await session.commit()

        # Cache must have been invalidated
        assert office_cache.get(tenant_id) is None

        # 3. Next fetch: repopulates cache from DB with the new office
        codes2 = await repo.get_valid_office_codes(tenant_id)
        assert office_code in codes2
        assert office_cache.get(tenant_id) == {office_code}
