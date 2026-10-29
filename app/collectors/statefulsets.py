"""Collector and parser for Kubernetes StatefulSet resources."""

from typing import Any, Dict, List, Optional


def _get(obj: Any, attr: str, default: Any = None) -> Any:
    """Helper to access attributes on objects or keys on dictionaries."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)


def parse_statefulset(sts: Any) -> Dict[str, Any]:
    """Parse a Kubernetes StatefulSet object into a standardized monitoring dictionary.

    Detects:
    - Replica shortages (readyReplicas < desiredReplicas)
    - Unready or updating replicas
    """
    metadata = _get(sts, "metadata")
    status = _get(sts, "status")
    spec = _get(sts, "spec")

    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")
    service_name = _get(spec, "service_name", _get(spec, "serviceName", ""))

    desired_replicas = int(_get(spec, "replicas", 1) or 0)
    current_replicas = int(_get(status, "current_replicas", _get(status, "currentReplicas", 0)) or 0)
    ready_replicas = int(_get(status, "ready_replicas", _get(status, "readyReplicas", 0)) or 0)
    updated_replicas = int(_get(status, "updated_replicas", _get(status, "updatedReplicas", 0)) or 0)
    created_at = _get(metadata, "creation_timestamp", _get(metadata, "creationTimestamp"))

    issues: List[Dict[str, Any]] = []

    # Check for replica shortages
    if desired_replicas > 0 and ready_replicas < desired_replicas:
        shortage = desired_replicas - ready_replicas
        issues.append({
            "type": "StatefulSetReplicaShortage",
            "message": f"StatefulSet {name} has {ready_replicas}/{desired_replicas} ready replicas ({shortage} missing)",
        })

    is_healthy = len(issues) == 0

    return {
        "namespace": namespace,
        "name": name,
        "serviceName": service_name,
        "desired": desired_replicas,
        "current": current_replicas,
        "ready": ready_replicas,
        "updated": updated_replicas,
        "isHealthy": is_healthy,
        "issues": issues,
        "createdAt": str(created_at) if created_at else None,
    }
