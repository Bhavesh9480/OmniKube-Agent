"""Mock object fixtures for unit testing Kubernetes resource collectors."""

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional


def make_mock_pod(
    namespace: str = "default",
    name: str = "test-pod",
    uid: str = "uid-12345",
    phase: str = "Running",
    node_name: Optional[str] = "worker-1",
    pod_ip: Optional[str] = "10.244.0.10",
    ready: bool = True,
    containers: Optional[List[Dict[str, Any]]] = None,
    created_at: Optional[datetime] = None,
    deletion_timestamp: Optional[datetime] = None,
    reason: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a mock Pod dictionary structure."""
    if created_at is None:
        created_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    if containers is None:
        containers = [
            {
                "name": "app",
                "image": "nginx:alpine",
                "ready": ready,
                "restartCount": 0,
                "state": {"running": {}},
            }
        ]

    conditions = [{"type": "Ready", "status": "True" if ready else "False"}]

    return {
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "creationTimestamp": created_at.isoformat(),
            "deletionTimestamp": deletion_timestamp.isoformat() if deletion_timestamp else None,
        },
        "spec": {
            "nodeName": node_name,
        },
        "status": {
            "phase": phase,
            "podIP": pod_ip,
            "hostIP": "10.0.0.1",
            "conditions": conditions,
            "containerStatuses": containers,
            "reason": reason,
            "message": message,
        },
    }


def make_mock_deployment(
    namespace: str = "default",
    name: str = "test-deployment",
    desired: int = 3,
    updated: int = 3,
    ready: int = 3,
    available: int = 3,
    unavailable: int = 0,
    generation: int = 1,
    observed_generation: int = 1,
    conditions: Optional[List[Dict[str, Any]]] = None,
    created_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Create a mock Deployment dictionary structure."""
    if created_at is None:
        created_at = datetime.now(timezone.utc) - timedelta(days=1)

    if conditions is None:
        conditions = [
            {"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable"}
        ]

    return {
        "metadata": {
            "namespace": namespace,
            "name": name,
            "generation": generation,
            "creationTimestamp": created_at.isoformat(),
        },
        "spec": {
            "replicas": desired,
        },
        "status": {
            "observedGeneration": observed_generation,
            "updatedReplicas": updated,
            "readyReplicas": ready,
            "availableReplicas": available,
            "unavailableReplicas": unavailable,
            "conditions": conditions,
        },
    }


def make_mock_endpointslice(
    namespace: str = "default",
    name: str = "test-service-slice-1",
    service_name: str = "test-service",
    endpoints: Optional[List[Dict[str, Any]]] = None,
    ports: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Create a mock EndpointSlice dictionary structure."""
    if endpoints is None:
        endpoints = [
            {"addresses": ["10.244.0.10"], "conditions": {"ready": True, "serving": True}},
            {"addresses": ["10.244.0.11"], "conditions": {"ready": True, "serving": True}},
        ]

    if ports is None:
        ports = [{"name": "http", "port": 80, "protocol": "TCP"}]

    return {
        "metadata": {
            "namespace": namespace,
            "name": name,
            "labels": {"kubernetes.io/service-name": service_name},
        },
        "addressType": "IPv4",
        "endpoints": endpoints,
        "ports": ports,
    }


def make_mock_service(
    namespace: str = "default",
    name: str = "test-service",
    service_type: str = "ClusterIP",
    cluster_ip: str = "10.96.0.100",
    selector: Optional[Dict[str, str]] = None,
    ports: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Create a mock Service dictionary structure."""
    if selector is None and service_type != "ExternalName":
        selector = {"app": "test-service"}
    elif selector is None:
        selector = {}

    if ports is None:
        ports = [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}]

    return {
        "metadata": {
            "namespace": namespace,
            "name": name,
            "creationTimestamp": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        },
        "spec": {
            "type": service_type,
            "clusterIP": cluster_ip,
            "selector": selector,
            "ports": ports,
        },
    }
