"""Collector and parser for Kubernetes Node resources.

Extracts node readiness, system pressures (Memory, Disk, PID),
allocatable capacity, and Kubelet version.
"""

from typing import Any, Dict, List, Optional


def _get(obj: Any, attr: str, default: Any = None) -> Any:
    """Helper to access attributes on objects or keys on dictionaries."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)


def parse_node(node: Any) -> Dict[str, Any]:
    """Parse a Kubernetes Node object into a standardized monitoring dictionary.

    Detects:
    - Node NotReady condition
    - MemoryPressure, DiskPressure, PIDPressure
    """
    metadata = _get(node, "metadata")
    status = _get(node, "status")
    spec = _get(node, "spec")

    name = _get(metadata, "name", "unknown")
    labels = _get(metadata, "labels", {}) or {}
    created_at = _get(metadata, "creation_timestamp", _get(metadata, "creationTimestamp"))

    # Conditions
    conditions = _get(status, "conditions", []) or []
    is_ready = False
    ready_reason = "Unknown"
    ready_message = ""
    pressures: List[str] = []
    issues: List[Dict[str, Any]] = []

    for cond in conditions:
        c_type = _get(cond, "type", "")
        c_status = str(_get(cond, "status", "")).lower()
        c_reason = _get(cond, "reason", "")
        c_message = _get(cond, "message", "")

        if c_type == "Ready":
            if c_status == "true":
                is_ready = True
                ready_reason = "KubeletReady"
            else:
                is_ready = False
                ready_reason = c_reason or "NodeNotReady"
                ready_message = c_message
                issues.append({
                    "type": "NodeNotReady",
                    "message": c_message or f"Node {name} is NotReady ({ready_reason})",
                })
        elif c_type in ("MemoryPressure", "DiskPressure", "PIDPressure") and c_status == "true":
            pressures.append(c_type)
            issues.append({
                "type": f"Node{c_type}",
                "message": c_message or f"Node {name} is experiencing {c_type}",
            })

    # Addresses
    addresses = _get(status, "addresses", []) or []
    internal_ip = None
    hostname = name
    for addr in addresses:
        a_type = _get(addr, "type", "")
        a_val = _get(addr, "address", "")
        if a_type == "InternalIP":
            internal_ip = a_val
        elif a_type == "Hostname":
            hostname = a_val

    # Node Info
    node_info = _get(status, "node_info", _get(status, "nodeInfo", {}))
    kubelet_version = _get(node_info, "kubelet_version", _get(node_info, "kubeletVersion", "unknown"))
    os_image = _get(node_info, "os_image", _get(node_info, "osImage", "unknown"))
    container_runtime = _get(
        node_info, "container_runtime_version", _get(node_info, "containerRuntimeVersion", "unknown")
    )

    # Capacity & Allocatable
    allocatable = _get(status, "allocatable", {}) or {}
    allocatable_cpu = _get(allocatable, "cpu", "unknown")
    allocatable_memory = _get(allocatable, "memory", "unknown")
    allocatable_pods = _get(allocatable, "pods", "unknown")

    roles: List[str] = []
    for k in labels.keys():
        if k.startswith("node-role.kubernetes.io/"):
            role = k.split("/", 1)[1]
            if role:
                roles.append(role)
    if not roles:
        roles = ["worker"]

    is_healthy = is_ready and len(pressures) == 0

    return {
        "name": name,
        "ready": is_ready,
        "isHealthy": is_healthy,
        "roles": roles,
        "internalIP": internal_ip,
        "hostname": hostname,
        "kubeletVersion": kubelet_version,
        "osImage": os_image,
        "containerRuntime": container_runtime,
        "allocatable": {
            "cpu": str(allocatable_cpu),
            "memory": str(allocatable_memory),
            "pods": str(allocatable_pods),
        },
        "pressures": pressures,
        "issues": issues,
        "createdAt": str(created_at) if created_at else None,
    }
