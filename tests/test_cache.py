"""Unit tests for ResourceCache thread-safety and staleness logic."""

import threading
import time
from datetime import datetime, timezone, timedelta
from app.cache import ResourceCache


def test_cache_freshness_and_staleness():
    cache = ResourceCache(stale_threshold_seconds=2)

    # Initially not fresh because initial sync is not completed
    assert cache.is_fresh() is False

    # Mark initial sync completed and set pods
    cache.set_initial_sync_completed(True)
    cache.set_pods([])

    # Fresh immediately after sync
    assert cache.is_fresh() is True
    assert cache.get_cache_age_seconds() is not None
    assert cache.get_cache_age_seconds() < 1.0

    # Simulate passage of time
    past_time = datetime.now(timezone.utc) - timedelta(seconds=5)
    with cache._lock:
        cache._overall_last_sync = past_time

    # Now stale because age (5s) > threshold (2s)
    assert cache.is_fresh() is False
    assert cache.get_cache_age_seconds() >= 4.0


def test_cache_concurrent_thread_safety():
    cache = ResourceCache()
    errors = []

    def writer_task(worker_id: int):
        try:
            for i in range(50):
                pod = {
                    "namespace": f"ns-{worker_id}",
                    "name": f"pod-{i}",
                    "phase": "Running",
                    "ready": True,
                }
                cache.upsert_pod(pod)
                if i % 10 == 0:
                    cache.delete_pod(f"ns-{worker_id}", f"pod-{i}")
        except Exception as e:
            errors.append(e)

    def reader_task():
        try:
            for _ in range(50):
                _ = cache.get_pods()
                _ = cache.get_stats()
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)

    threads = []
    for wid in range(4):
        threads.append(threading.Thread(target=writer_task, args=(wid,)))
    for _ in range(4):
        threads.append(threading.Thread(target=reader_task))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    assert len(cache.get_pods()) > 0
