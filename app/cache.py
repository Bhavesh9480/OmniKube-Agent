"""Thread-safe in-memory cache for Kubernetes cluster state."""

import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


class ResourceCache:
    """Thread-safe in-memory storage for discovered Kubernetes resources."""

    def __init__(self, stale_threshold_seconds: int = 120):
        self._lock = threading.RLock()
        self.stale_threshold_seconds = stale_threshold_seconds

        # Key: (namespace, name) -> Value: resource dictionary
        self._pods: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._deployments: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._services: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._endpointslices: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._events: List[Dict[str, Any]] = []

        # Synchronization metadata
        self._last_sync_time: Dict[str, datetime] = {}
        self._overall_last_sync: Optional[datetime] = None
        self._initial_sync_completed: bool = False
        self._sync_errors: Dict[str, str] = {}

    def _broadcast(self, fn) -> None:
        try:
            from app.broadcaster import broadcaster
            fn(broadcaster)
        except Exception:
            pass

    # --- Pods ---
    def set_pods(self, pods: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._pods = {(p["namespace"], p["name"]): p for p in pods}
            now = datetime.now(timezone.utc)
            self._last_sync_time["pods"] = now
            self._overall_last_sync = now
            self._sync_errors.pop("pods", None)
        self._broadcast(lambda b: b.broadcast({"type": "resources_synced", "resourceKind": "Pod", "count": len(pods)}))

    def upsert_pod(self, pod: Dict[str, Any]) -> None:
        with self._lock:
            self._pods[(pod["namespace"], pod["name"])] = pod
            now = datetime.now(timezone.utc)
            self._last_sync_time["pods"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_updated("Pod", pod))

    def delete_pod(self, namespace: str, name: str) -> None:
        with self._lock:
            self._pods.pop((namespace, name), None)
            now = datetime.now(timezone.utc)
            self._last_sync_time["pods"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_deleted("Pod", namespace, name))

    def get_pods(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._pods.values())

    # --- Deployments ---
    def set_deployments(self, deployments: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._deployments = {(d["namespace"], d["name"]): d for d in deployments}
            now = datetime.now(timezone.utc)
            self._last_sync_time["deployments"] = now
            self._overall_last_sync = now
            self._sync_errors.pop("deployments", None)
        self._broadcast(lambda b: b.broadcast({"type": "resources_synced", "resourceKind": "Deployment", "count": len(deployments)}))

    def upsert_deployment(self, deployment: Dict[str, Any]) -> None:
        with self._lock:
            self._deployments[(deployment["namespace"], deployment["name"])] = deployment
            now = datetime.now(timezone.utc)
            self._last_sync_time["deployments"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_updated("Deployment", deployment))

    def delete_deployment(self, namespace: str, name: str) -> None:
        with self._lock:
            self._deployments.pop((namespace, name), None)
            now = datetime.now(timezone.utc)
            self._last_sync_time["deployments"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_deleted("Deployment", namespace, name))

    def get_deployments(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._deployments.values())

    # --- Services ---
    def set_services(self, services: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._services = {(s["namespace"], s["name"]): s for s in services}
            now = datetime.now(timezone.utc)
            self._last_sync_time["services"] = now
            self._overall_last_sync = now
            self._sync_errors.pop("services", None)
        self._broadcast(lambda b: b.broadcast({"type": "resources_synced", "resourceKind": "Service", "count": len(services)}))

    def upsert_service(self, service: Dict[str, Any]) -> None:
        with self._lock:
            self._services[(service["namespace"], service["name"])] = service
            now = datetime.now(timezone.utc)
            self._last_sync_time["services"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_updated("Service", service))

    def delete_service(self, namespace: str, name: str) -> None:
        with self._lock:
            self._services.pop((namespace, name), None)
            now = datetime.now(timezone.utc)
            self._last_sync_time["services"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_deleted("Service", namespace, name))

    def get_services(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._services.values())

    # --- EndpointSlices ---
    def set_endpointslices(self, slices: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._endpointslices = {(s["namespace"], s["name"]): s for s in slices}
            now = datetime.now(timezone.utc)
            self._last_sync_time["endpointslices"] = now
            self._overall_last_sync = now
            self._sync_errors.pop("endpointslices", None)
        self._broadcast(lambda b: b.broadcast({"type": "resources_synced", "resourceKind": "EndpointSlice", "count": len(slices)}))

    def upsert_endpointslice(self, slice_obj: Dict[str, Any]) -> None:
        with self._lock:
            self._endpointslices[(slice_obj["namespace"], slice_obj["name"])] = slice_obj
            now = datetime.now(timezone.utc)
            self._last_sync_time["endpointslices"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_updated("EndpointSlice", slice_obj))

    def delete_endpointslice(self, namespace: str, name: str) -> None:
        with self._lock:
            self._endpointslices.pop((namespace, name), None)
            now = datetime.now(timezone.utc)
            self._last_sync_time["endpointslices"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast_resource_deleted("EndpointSlice", namespace, name))

    def get_endpointslices(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._endpointslices.values())

    # --- Events ---
    def set_events(self, events: List[Dict[str, Any]], limit: int = 500) -> None:
        with self._lock:
            # Keep bounded and sorted by last_timestamp or first_timestamp desc
            self._events = events[:limit]
            now = datetime.now(timezone.utc)
            self._last_sync_time["events"] = now
            self._overall_last_sync = now
            self._sync_errors.pop("events", None)

    def add_event(self, event: Dict[str, Any], limit: int = 500) -> None:
        with self._lock:
            self._events.insert(0, event)
            if len(self._events) > limit:
                self._events.pop()
            now = datetime.now(timezone.utc)
            self._last_sync_time["events"] = now
            self._overall_last_sync = now
        self._broadcast(lambda b: b.broadcast({"type": "event_created", "timestamp": now.isoformat(), "data": event}))

    def get_events(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._events)

    # --- Sync Status & Health ---
    def record_sync_error(self, resource_type: str, error: str) -> None:
        with self._lock:
            self._sync_errors[resource_type] = error
        self._broadcast(lambda b: b.broadcast_sync_status("error"))

    def clear_sync_error(self, resource_type: str) -> None:
        with self._lock:
            self._sync_errors.pop(resource_type, None)

    def get_sync_errors(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._sync_errors)

    def set_initial_sync_completed(self, completed: bool = True) -> None:
        with self._lock:
            self._initial_sync_completed = completed
        last_sync = self.get_last_successful_sync()
        self._broadcast(lambda b: b.broadcast_sync_status("synchronized" if completed else "syncing", last_sync.isoformat() if last_sync else None))

    def is_initial_sync_completed(self) -> bool:
        with self._lock:
            return self._initial_sync_completed

    def get_last_successful_sync(self) -> Optional[datetime]:
        with self._lock:
            return self._overall_last_sync

    def is_fresh(self, threshold_seconds: Optional[int] = None) -> bool:
        """Check if cache data was refreshed within the given threshold."""
        threshold = threshold_seconds if threshold_seconds is not None else self.stale_threshold_seconds
        with self._lock:
            if not self._initial_sync_completed or self._overall_last_sync is None:
                return False
            now = datetime.now(timezone.utc)
            age = (now - self._overall_last_sync).total_seconds()
            return age <= threshold

    def get_cache_age_seconds(self) -> Optional[float]:
        with self._lock:
            if self._overall_last_sync is None:
                return None
            return (datetime.now(timezone.utc) - self._overall_last_sync).total_seconds()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "pods_count": len(self._pods),
                "deployments_count": len(self._deployments),
                "services_count": len(self._services),
                "endpointslices_count": len(self._endpointslices),
                "events_count": len(self._events),
                "initial_sync_completed": self._initial_sync_completed,
                "overall_last_sync": self._overall_last_sync.isoformat() if self._overall_last_sync else None,
                "sync_errors": dict(self._sync_errors),
            }

    def clear(self) -> None:
        with self._lock:
            self._pods.clear()
            self._deployments.clear()
            self._services.clear()
            self._endpointslices.clear()
            self._events.clear()
            self._last_sync_time.clear()
            self._overall_last_sync = None
            self._initial_sync_completed = False
            self._sync_errors.clear()


# Global cache instance
resource_cache = ResourceCache()
