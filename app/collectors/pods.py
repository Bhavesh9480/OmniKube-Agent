"""Collector and parser for Kubernetes Pod resources."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _get(obj: Any, attr: str, default: Any = None) -> Any:
    """Helper to access attributes on objects or keys on dictionaries."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)


def _format_datetime(dt: Any) -> Optional[str]:
    """Convert datetime object or string to ISO 8601 string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(dt)


def _parse_datetime(dt: Any) -> Optional[datetime]:
    """Parse string or datetime object to UTC datetime."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    try:
        from dateutil import parser
        parsed = parser.parse(str(dt))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def parse_container_status(status: Any) -> Dict[str, Any]:
    """Extract structured container status details."""
    name = _get(status, "name", "unknown")
    image = _get(status, "image", "")
    ready = bool(_get(status, "ready", False))
    restart_count = int(_get(status, "restart_count", _get(status, "restartCount", 0)))

    state_obj = _get(status, "state")
    state_str = "unknown"
    reason = None
    message = None
    exit_code = None

    if state_obj:
        running = _get(state_obj, "running")
        waiting = _get(state_obj, "waiting")
        terminated = _get(state_obj, "terminated")

        if running is not None:
            state_str = "running"
        elif waiting is not None:
            state_str = "waiting"
            reason = _get(waiting, "reason")
            message = _get(waiting, "message")
        elif terminated is not None:
            state_str = "terminated"
            reason = _get(terminated, "reason")
            message = _get(terminated, "message")
            exit_code = _get(terminated, "exit_code", _get(terminated, "exitCode"))

    return {
        "name": name,
        "image": image,
        "ready": ready,
        "restartCount": restart_count,
        "state": state_str,
        "reason": reason,
        "message": message,
        "exitCode": exit_code,
    }


def parse_pod(
    pod: Any,
    pending_threshold_seconds: int = 300,
    crash_loop_restart_threshold: int = 3,
) -> Dict[str, Any]:
    """Parse a Kubernetes Pod object into a standardized monitoring dictionary.
    
    Detects unhealthy conditions:
    - Pending pods exceeding pending_threshold_seconds
    - Failed pods
    - Unknown pod phases
    - Running pods with unready containers
    - CrashLoopBackOff, ImagePullBackOff, ErrImagePull
    - CreateContainerError, CreateContainerConfigError
    - Unexpected container termination
    - Repeated container restarts
    - Pods stuck terminating
    """
    metadata = _get(pod, "metadata")
    status = _get(pod, "status")
    spec = _get(pod, "spec")

    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")
    uid = _get(metadata, "uid", "")
    node = _get(spec, "node_name", _get(spec, "nodeName", _get(status, "host_ip", _get(status, "hostIP"))))
    pod_ip = _get(status, "pod_ip", _get(status, "podIP"))
    phase = _get(status, "phase", "Unknown")

    created_at = _get(metadata, "creation_timestamp", _get(metadata, "creationTimestamp"))
    deletion_timestamp = _get(metadata, "deletion_timestamp", _get(metadata, "deletionTimestamp"))

    # Conditions
    conditions = _get(status, "conditions", []) or []
    pod_ready_condition = False
    for cond in conditions:
        c_type = _get(cond, "type")
        c_status = _get(cond, "status")
        if c_type == "Ready" and str(c_status).lower() == "true":
            pod_ready_condition = True
            break

    # Containers
    container_statuses = (
        _get(status, "container_statuses", _get(status, "containerStatuses", [])) or []
    )
    init_container_statuses = (
        _get(status, "init_container_statuses", _get(status, "initContainerStatuses", [])) or []
    )

    containers: List[Dict[str, Any]] = []
    ready_containers_count = 0
    total_restarts = 0
    issues: List[Dict[str, Any]] = []

    # Check container statuses
    all_statuses = list(container_statuses)
    for c_stat in all_statuses:
        parsed_c = parse_container_status(c_stat)
        containers.append(parsed_c)
        if parsed_c["ready"]:
            ready_containers_count += 1
        total_restarts += parsed_c["restartCount"]

        # Check for specific container failure reasons
        c_reason = parsed_c.get("reason") or ""
        c_name = parsed_c["name"]

        # Waiting reasons
        if parsed_c["state"] == "waiting":
            if c_reason in (
                "CrashLoopBackOff",
                "ImagePullBackOff",
                "ErrImagePull",
                "CreateContainerError",
                "CreateContainerConfigError",
            ):
                issues.append({
                    "type": c_reason,
                    "container": c_name,
                    "message": parsed_c.get("message") or f"Container {c_name} waiting due to {c_reason}",
                })
            elif c_reason:
                issues.append({
                    "type": "ContainerWaiting",
                    "container": c_name,
                    "message": f"Container {c_name} waiting: {c_reason}",
                })

        # Terminated reasons
        if parsed_c["state"] == "terminated":
            exit_code = parsed_c.get("exitCode")
            if exit_code is not None and exit_code != 0:
                issues.append({
                    "type": "ContainerTerminatedError",
                    "container": c_name,
                    "message": f"Container {c_name} terminated with exit code {exit_code}: {c_reason}",
                })

        # High restarts
        if parsed_c["restartCount"] >= crash_loop_restart_threshold:
            issues.append({
                "type": "HighRestartCount",
                "container": c_name,
                "message": f"Container {c_name} has restarted {parsed_c['restartCount']} times",
            })

    total_containers_count = len(containers)

    # Check init containers for errors
    for init_stat in init_container_statuses:
        parsed_init = parse_container_status(init_stat)
        c_reason = parsed_init.get("reason") or ""
        if parsed_init["state"] == "waiting" and c_reason in (
            "CrashLoopBackOff",
            "ImagePullBackOff",
            "ErrImagePull",
            "CreateContainerError",
            "CreateContainerConfigError",
        ):
            issues.append({
                "type": f"InitContainer{c_reason}",
                "container": parsed_init["name"],
                "message": parsed_init.get("message") or f"Init container {parsed_init['name']} waiting: {c_reason}",
            })

    # Pod level status & issues
    now = datetime.now(timezone.utc)
    is_terminating = deletion_timestamp is not None
    if is_terminating:
        issues.append({
            "type": "PodTerminating",
            "message": "Pod is currently terminating",
        })

    if phase == "Pending":
        created_dt = _parse_datetime(created_at)
        pending_duration = (now - created_dt).total_seconds() if created_dt else 0
        if pending_duration > pending_threshold_seconds:
            issues.append({
                "type": "ProlongedPending",
                "message": f"Pod has been Pending for {int(pending_duration)}s (exceeds {pending_threshold_seconds}s threshold)",
            })
        else:
            issues.append({
                "type": "PodPending",
                "message": "Pod is in Pending phase",
            })
    elif phase == "Failed":
        reason = _get(status, "reason", "PodFailed")
        msg = _get(status, "message", "Pod execution failed")
        issues.append({
            "type": "PodFailed",
            "message": f"{reason}: {msg}",
        })
    elif phase == "Unknown":
        issues.append({
            "type": "PodPhaseUnknown",
            "message": "Pod phase is Unknown",
        })
    elif phase == "Running":
        # Do not assume running pod is healthy!
        if total_containers_count > 0 and ready_containers_count < total_containers_count:
            issues.append({
                "type": "ContainersNotReady",
                "message": f"{ready_containers_count}/{total_containers_count} containers ready",
            })

    # Overall readiness
    is_ready = pod_ready_condition and (
        ready_containers_count == total_containers_count if total_containers_count > 0 else True
    )

    # Unhealthy determination
    is_unhealthy = len(issues) > 0 or not is_ready or phase not in ("Running", "Succeeded")

    # Primary reason summary
    primary_reason = issues[0]["type"] if issues else (phase if is_ready else "NotReady")

    return {
        "namespace": namespace,
        "name": name,
        "uid": str(uid),
        "phase": phase,
        "node": node,
        "podIP": pod_ip,
        "ready": is_ready,
        "readyContainers": ready_containers_count,
        "totalContainers": total_containers_count,
        "restarts": total_restarts,
        "reason": primary_reason,
        "containers": containers,
        "createdAt": _format_datetime(created_at),
        "deletionTimestamp": _format_datetime(deletion_timestamp),
        "isTerminating": is_terminating,
        "isUnhealthy": is_unhealthy,
        "issues": issues,
    }
