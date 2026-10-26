"""Collector and parser for Kubernetes Deployment resources."""

from typing import Any, Dict, List, Optional
from app.collectors.pods import _get, _format_datetime


def parse_deployment(deployment: Any) -> Dict[str, Any]:
    """Parse a Kubernetes Deployment object into a standardized monitoring dictionary.
    
    Detects unhealthy conditions:
    - Replica shortages (ready < desired or available < desired when desired > 0)
    - Unavailable replicas > 0
    - Progress failures (ProgressDeadlineExceeded or Progressing==False)
    - Stale observed generation (generation > observed_generation)
    - Handles zero-replica deployments correctly (desired == 0 is healthy)
    """
    metadata = _get(deployment, "metadata")
    status = _get(deployment, "status")
    spec = _get(deployment, "spec")

    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")
    generation = _get(metadata, "generation")
    created_at = _get(metadata, "creation_timestamp", _get(metadata, "creationTimestamp"))

    # Spec replicas (desired)
    desired = _get(spec, "replicas", 1)
    if desired is None:
        desired = 1
    desired = int(desired)

    # Status replicas
    updated = int(_get(status, "updated_replicas", _get(status, "updatedReplicas", 0)) or 0)
    ready = int(_get(status, "ready_replicas", _get(status, "readyReplicas", 0)) or 0)
    available = int(_get(status, "available_replicas", _get(status, "availableReplicas", 0)) or 0)
    unavailable = int(_get(status, "unavailable_replicas", _get(status, "unavailableReplicas", 0)) or 0)
    observed_generation = _get(status, "observed_generation", _get(status, "observedGeneration"))

    # Conditions
    raw_conditions = _get(status, "conditions", []) or []
    conditions: List[Dict[str, Any]] = []
    progress_failed = False
    progress_reason = None
    progress_message = None

    for cond in raw_conditions:
        c_type = _get(cond, "type")
        c_status = str(_get(cond, "status")).lower()
        c_reason = _get(cond, "reason")
        c_msg = _get(cond, "message")

        conditions.append({
            "type": c_type,
            "status": c_status,
            "reason": c_reason,
            "message": c_msg,
        })

        if c_type == "Progressing":
            if c_status == "false" or c_reason == "ProgressDeadlineExceeded":
                progress_failed = True
                progress_reason = c_reason or "ProgressDeadlineExceeded"
                progress_message = c_msg or "Deployment progress deadline exceeded"

    issues: List[Dict[str, Any]] = []

    # Handle zero-replica deployments: desired == 0 is completely normal and healthy
    if desired == 0:
        is_healthy = True
        status_label = "scaled_to_zero"
        reason = "ScaledToZero"
    else:
        # Check replica shortages
        if unavailable > 0:
            issues.append({
                "type": "UnavailableReplicas",
                "message": f"{unavailable} replicas are currently unavailable",
            })
        if available < desired:
            issues.append({
                "type": "ReplicaShortage",
                "message": f"Available replicas ({available}) less than desired ({desired})",
            })
        if ready < desired:
            issues.append({
                "type": "UnreadyReplicas",
                "message": f"Ready replicas ({ready}) less than desired ({desired})",
            })

        # Check progress failure
        if progress_failed:
            issues.append({
                "type": "ProgressFailure",
                "message": f"Deployment progress failed: {progress_reason} - {progress_message}",
            })

        # Check stale generation
        if generation is not None and observed_generation is not None:
            if generation > observed_generation:
                issues.append({
                    "type": "StaleObservedGeneration",
                    "message": f"Observed generation ({observed_generation}) lags behind desired generation ({generation})",
                })

        is_healthy = len(issues) == 0
        status_label = "healthy" if is_healthy else "unhealthy"
        reason = issues[0]["type"] if issues else "Available"

    return {
        "namespace": namespace,
        "name": name,
        "desired": desired,
        "updated": updated,
        "ready": ready,
        "available": available,
        "unavailable": unavailable,
        "status": status_label,
        "reason": reason,
        "generation": generation,
        "observedGeneration": observed_generation,
        "createdAt": _format_datetime(created_at),
        "isHealthy": is_healthy,
        "issues": issues,
        "conditions": conditions,
    }
