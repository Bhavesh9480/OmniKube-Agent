"""Collector and parser for Kubernetes EndpointSlice resources."""

from typing import Any, Dict, List
from app.collectors.pods import _get


def parse_endpointslice(endpoint_slice: Any) -> Dict[str, Any]:
    """Parse a Kubernetes DiscoveryV1 EndpointSlice object."""
    metadata = _get(endpoint_slice, "metadata")
    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")
    labels = _get(metadata, "labels", {}) or {}

    # Standard label identifying the associated service
    service_name = labels.get("kubernetes.io/service-name")

    address_type = _get(endpoint_slice, "address_type", _get(endpoint_slice, "addressType", "IPv4"))

    endpoints_raw = _get(endpoint_slice, "endpoints", []) or []
    endpoints: List[Dict[str, Any]] = []
    ready_endpoints_count = 0

    for ep in endpoints_raw:
        addresses = _get(ep, "addresses", []) or []
        conditions = _get(ep, "conditions", {}) or {}

        # Conditions: ready, serving, terminating
        ready = _get(conditions, "ready", False)
        if ready is None:
            ready = False
        serving = _get(conditions, "serving", ready)
        terminating = _get(conditions, "terminating", False)

        if ready:
            ready_endpoints_count += 1

        node_name = _get(ep, "node_name", _get(ep, "nodeName"))
        target_ref = _get(ep, "target_ref", _get(ep, "targetRef"))

        endpoints.append({
            "addresses": list(addresses),
            "ready": bool(ready),
            "serving": bool(serving),
            "terminating": bool(terminating),
            "nodeName": node_name,
            "targetRef": {
                "kind": _get(target_ref, "kind"),
                "name": _get(target_ref, "name"),
                "namespace": _get(target_ref, "namespace"),
            } if target_ref else None,
        })

    ports_raw = _get(endpoint_slice, "ports", []) or []
    ports: List[Dict[str, Any]] = []
    for p in ports_raw:
        ports.append({
            "name": _get(p, "name"),
            "port": _get(p, "port"),
            "protocol": _get(p, "protocol", "TCP"),
            "appProtocol": _get(p, "app_protocol", _get(p, "appProtocol")),
        })

    return {
        "namespace": namespace,
        "name": name,
        "serviceName": service_name,
        "addressType": address_type,
        "endpoints": endpoints,
        "ports": ports,
        "readyCount": ready_endpoints_count,
        "totalEndpoints": len(endpoints),
    }
