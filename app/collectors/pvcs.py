"""Collector and parser for Kubernetes PersistentVolumeClaim (PVC) resources."""

from typing import Any, Dict, List, Optional


def _get(obj: Any, attr: str, default: Any = None) -> Any:
    """Helper to access attributes on objects or keys on dictionaries."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)


def parse_pvc(pvc: Any) -> Dict[str, Any]:
    """Parse a Kubernetes PersistentVolumeClaim into a standardized monitoring dictionary.

    Detects:
    - Pending storage bindings
    - Lost volume associations
    """
    metadata = _get(pvc, "metadata")
    status = _get(pvc, "status")
    spec = _get(pvc, "spec")

    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")
    storage_class = _get(spec, "storage_class_name", _get(spec, "storageClassName", ""))
    volume_name = _get(spec, "volume_name", _get(spec, "volumeName", ""))
    phase = _get(status, "phase", "Unknown")
    created_at = _get(metadata, "creation_timestamp", _get(metadata, "creationTimestamp"))

    resources = _get(spec, "resources", {}) or {}
    requests = _get(resources, "requests", {}) or {}
    requested_storage = _get(requests, "storage", "unknown")

    capacity = _get(status, "capacity", {}) or {}
    actual_capacity = _get(capacity, "storage", requested_storage)

    issues: List[Dict[str, Any]] = []

    if phase == "Pending":
        issues.append({
            "type": "PVCPending",
            "message": f"PersistentVolumeClaim {name} is stuck in Pending phase waiting for volume binding",
        })
    elif phase == "Lost":
        issues.append({
            "type": "PVCLost",
            "message": f"PersistentVolumeClaim {name} lost its underlying PersistentVolume binding",
        })

    is_healthy = phase == "Bound" and len(issues) == 0

    return {
        "namespace": namespace,
        "name": name,
        "phase": phase,
        "storageClass": storage_class,
        "volumeName": volume_name,
        "requestedStorage": str(requested_storage),
        "capacity": str(actual_capacity),
        "isHealthy": is_healthy,
        "issues": issues,
        "createdAt": str(created_at) if created_at else None,
    }
