"""Health check diagnostics and probe evaluations."""

from typing import Any, Dict, Tuple
from app.cache import resource_cache
from app.config import settings
from app.kubernetes_client import k8s_manager


def check_liveness() -> Dict[str, str]:
    """Check whether the agent process is alive.
    
    A temporary Kubernetes API outage alone must not cause liveness failure.
    """
    return {"status": "ok"}


def check_readiness() -> Tuple[bool, Dict[str, Any]]:
    """Evaluate whether the agent is ready to serve reliable traffic.
    
    Requirements for readiness:
    1. In production/local-cluster mode, client must be connected.
    2. Initial resource synchronization must be completed.
    3. Cache data must not be stale (within cache_stale_threshold_seconds).
    
    Returns:
        (is_ready, details_dict)
    """
    is_mock = settings.app_mode == "mock"
    is_connected = k8s_manager.connected or is_mock
    sync_completed = resource_cache.is_initial_sync_completed()
    is_fresh = resource_cache.is_fresh(settings.cache_stale_threshold_seconds)

    if not is_connected:
        return False, {
            "status": "not_ready",
            "reason": "Kubernetes API client is not connected",
            "mode": settings.app_mode,
            "error": k8s_manager.last_error,
        }

    if not sync_completed:
        return False, {
            "status": "not_ready",
            "reason": "Initial cluster synchronization has not completed",
        }

    if not is_fresh:
        age = resource_cache.get_cache_age_seconds()
        return False, {
            "status": "not_ready",
            "reason": f"Cached resource state is stale ({age:.1f}s > threshold {settings.cache_stale_threshold_seconds}s)",
        }

    return True, {"status": "ready"}


def get_detailed_health(watcher_status: Dict[str, str]) -> Dict[str, Any]:
    """Return comprehensive diagnostics for /api/health without leaking secrets."""
    is_mock = settings.app_mode == "mock"
    is_connected = k8s_manager.connected or is_mock
    sync_completed = resource_cache.is_initial_sync_completed()
    is_fresh = resource_cache.is_fresh(settings.cache_stale_threshold_seconds)
    cache_age = resource_cache.get_cache_age_seconds()

    status_str = "healthy"
    if not is_connected or not sync_completed:
        status_str = "unhealthy"
    elif not is_fresh or len(resource_cache.get_sync_errors()) > 0:
        status_str = "degraded"

    return {
        "status": status_str,
        "agentVersion": settings.agent_version,
        "mode": settings.app_mode,
        "isMock": is_mock,
        "kubernetes": {
            "connected": is_connected,
            "mode": k8s_manager.mode,
            "lastError": k8s_manager.last_error,
        },
        "cache": {
            "initialSyncCompleted": sync_completed,
            "isFresh": is_fresh,
            "ageSeconds": round(cache_age, 2) if cache_age is not None else None,
            "staleThresholdSeconds": settings.cache_stale_threshold_seconds,
            "lastSuccessfulSync": (
                resource_cache.get_last_successful_sync().isoformat()
                if resource_cache.get_last_successful_sync()
                else None
            ),
            "stats": resource_cache.get_stats(),
        },
        "watchers": watcher_status,
        "syncErrors": resource_cache.get_sync_errors(),
    }
