"""Collector and parser for Kubernetes Service resources."""

from typing import Any, Dict, List, Optional
from app.collectors.pods import _get, _format_datetime


def parse_service(
    service: Any,
    endpoint_slices: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Parse a Kubernetes Service object and associate with EndpointSlices.
    
    Detects services expected to have backends (selector defined, not ExternalName)
    that lack ready endpoints in their associated EndpointSlices.
    Does NOT classify selector-less services (ExternalName, manual headless) as broken.
    """
    metadata = _get(service, "metadata")
    spec = _get(service, "spec")
    status = _get(service, "status")

    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")
    created_at = _get(metadata, "creation_timestamp", _get(metadata, "creationTimestamp"))

    service_type = _get(spec, "type", "ClusterIP")
    cluster_ip = _get(spec, "cluster_ip", _get(spec, "clusterIP"))
    selector = _get(spec, "selector", {}) or {}

    ports_raw = _get(spec, "ports", []) or []
    ports: List[Dict[str, Any]] = []
    for p in ports_raw:
        ports.append({
            "name": _get(p, "name"),
            "port": _get(p, "port"),
            "targetPort": _get(p, "target_port", _get(p, "targetPort")),
            "protocol": _get(p, "protocol", "TCP"),
            "nodePort": _get(p, "node_port", _get(p, "nodePort")),
        })

    # Find associated endpoint slices
    associated_ready_endpoints = 0
    associated_total_endpoints = 0
    slice_count = 0

    if endpoint_slices:
        for ep_slice in endpoint_slices:
            if ep_slice.get("namespace") == namespace and ep_slice.get("serviceName") == name:
                slice_count += 1
                associated_ready_endpoints += ep_slice.get("readyCount", 0)
                associated_total_endpoints += ep_slice.get("totalEndpoints", 0)

    # Health logic
    has_selector = bool(selector and len(selector) > 0)
    is_external_name = service_type == "ExternalName"
    expects_endpoints = has_selector and not is_external_name

    issues: List[Dict[str, Any]] = []
    without_ready_endpoints = False

    if expects_endpoints and slice_count > 0 and associated_ready_endpoints == 0:
        without_ready_endpoints = True
        issues.append({
            "type": "MissingReadyEndpoints",
            "message": f"Service specifies selector {selector} but has 0 ready endpoints across {slice_count} EndpointSlice(s)",
        })
    elif expects_endpoints and slice_count == 0:
        # If endpoint slices have been collected, but none exist for this service
        without_ready_endpoints = True
        issues.append({
            "type": "NoEndpointSlices",
            "message": f"Service specifies selector {selector} but has no associated EndpointSlices",
        })

    is_healthy = not without_ready_endpoints

    return {
        "namespace": namespace,
        "name": name,
        "type": service_type,
        "clusterIP": cluster_ip,
        "ports": ports,
        "selector": selector,
        "createdAt": _format_datetime(created_at),
        "expectsEndpoints": expects_endpoints,
        "hasSelector": has_selector,
        "readyEndpoints": associated_ready_endpoints,
        "totalEndpoints": associated_total_endpoints,
        "withoutReadyEndpoints": without_ready_endpoints,
        "isHealthy": is_healthy,
        "issues": issues,
    }
