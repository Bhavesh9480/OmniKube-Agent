"""Kubernetes resource watchers, reconciliation loop, and mock simulation."""

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from app.alerts import alert_manager
from app.cache import resource_cache
from app.collectors.deployments import parse_deployment
from app.collectors.endpointslices import parse_endpointslice
from app.collectors.events import parse_event
from app.collectors.pods import parse_pod
from app.collectors.services import parse_service
from app.config import settings
from app.kubernetes_client import ApiException, k8s_manager, watch

logger = logging.getLogger(__name__)


def generate_mock_cluster_data() -> Dict[str, Any]:
    """Generate realistic simulated cluster data for mock mode."""
    now = datetime.now(timezone.utc)

    # 1. Mock Pods
    raw_pods = [
        {
            "metadata": {
                "namespace": "kube-system",
                "name": "kube-apiserver-control-plane",
                "uid": "uid-apiserver-1",
                "creationTimestamp": (now - timedelta(days=10)).isoformat(),
            },
            "spec": {"nodeName": "control-plane-node"},
            "status": {
                "phase": "Running",
                "hostIP": "10.0.0.1",
                "podIP": "10.0.0.1",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [
                    {
                        "name": "kube-apiserver",
                        "image": "registry.k8s.io/kube-apiserver:v1.29.2",
                        "ready": True,
                        "restartCount": 0,
                        "state": {"running": {}},
                    }
                ],
            },
        },
        {
            "metadata": {
                "namespace": "kube-system",
                "name": "coredns-7689f8bdf-w84xk",
                "uid": "uid-coredns-1",
                "creationTimestamp": (now - timedelta(days=5)).isoformat(),
            },
            "spec": {"nodeName": "worker-node-1"},
            "status": {
                "phase": "Running",
                "hostIP": "10.0.0.2",
                "podIP": "10.244.0.5",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [
                    {
                        "name": "coredns",
                        "image": "registry.k8s.io/coredns/coredns:v1.11.1",
                        "ready": True,
                        "restartCount": 0,
                        "state": {"running": {}},
                    }
                ],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "api-service-78f9b6-abc12",
                "uid": "uid-api-1",
                "creationTimestamp": (now - timedelta(hours=8)).isoformat(),
            },
            "spec": {"nodeName": "worker-node-1"},
            "status": {
                "phase": "Running",
                "hostIP": "10.0.0.2",
                "podIP": "10.244.0.15",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [
                    {
                        "name": "api",
                        "image": "myregistry.io/api:v1.4.2",
                        "ready": True,
                        "restartCount": 1,
                        "state": {"running": {}},
                    },
                    {
                        "name": "envoy-sidecar",
                        "image": "envoyproxy/envoy:v1.28.0",
                        "ready": True,
                        "restartCount": 0,
                        "state": {"running": {}},
                    },
                ],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "payment-gateway-9df8b-p98a",
                "uid": "uid-pay-1",
                "creationTimestamp": (now - timedelta(hours=2)).isoformat(),
            },
            "spec": {"nodeName": "worker-node-2"},
            "status": {
                "phase": "Running",
                "hostIP": "10.0.0.3",
                "podIP": "10.244.1.22",
                "conditions": [{"type": "Ready", "status": "False"}],
                "containerStatuses": [
                    {
                        "name": "payment-worker",
                        "image": "myregistry.io/payment:v2.1.0",
                        "ready": False,
                        "restartCount": 6,
                        "state": {
                            "waiting": {
                                "reason": "CrashLoopBackOff",
                                "message": "Back-off 5m0s restarting failed container=payment-worker",
                            }
                        },
                    }
                ],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "frontend-web-66c5d9-48kj",
                "uid": "uid-front-1",
                "creationTimestamp": (now - timedelta(minutes=45)).isoformat(),
            },
            "spec": {"nodeName": "worker-node-2"},
            "status": {
                "phase": "Pending",
                "hostIP": "10.0.0.3",
                "podIP": None,
                "conditions": [{"type": "Ready", "status": "False"}],
                "containerStatuses": [
                    {
                        "name": "web",
                        "image": "myregistry.io/frontend:non-existent-tag",
                        "ready": False,
                        "restartCount": 0,
                        "state": {
                            "waiting": {
                                "reason": "ImagePullBackOff",
                                "message": "Back-off pulling image 'myregistry.io/frontend:non-existent-tag'",
                            }
                        },
                    }
                ],
            },
        },
        {
            "metadata": {
                "namespace": "staging",
                "name": "report-batch-job-11a2c",
                "uid": "uid-batch-1",
                "creationTimestamp": (now - timedelta(minutes=15)).isoformat(),
            },
            "spec": {"nodeName": None},
            "status": {
                "phase": "Pending",
                "hostIP": None,
                "podIP": None,
                "conditions": [{"type": "PodScheduled", "status": "False"}],
                "containerStatuses": [],
            },
        },
        {
            "metadata": {
                "namespace": "staging",
                "name": "stale-worker-9988-q1w2",
                "uid": "uid-stale-1",
                "creationTimestamp": (now - timedelta(hours=3)).isoformat(),
                "deletionTimestamp": (now - timedelta(minutes=10)).isoformat(),
            },
            "spec": {"nodeName": "worker-node-1"},
            "status": {
                "phase": "Running",
                "hostIP": "10.0.0.2",
                "podIP": "10.244.0.33",
                "conditions": [{"type": "Ready", "status": "False"}],
                "containerStatuses": [
                    {
                        "name": "worker",
                        "image": "myregistry.io/worker:v1.0",
                        "ready": False,
                        "restartCount": 0,
                        "state": {"running": {}},
                    }
                ],
            },
        },
    ]

    # 2. Mock EndpointSlices
    raw_endpointslices = [
        {
            "metadata": {
                "namespace": "production",
                "name": "api-service-slice-1",
                "labels": {"kubernetes.io/service-name": "api-service"},
            },
            "addressType": "IPv4",
            "endpoints": [
                {"addresses": ["10.244.0.15"], "conditions": {"ready": True}},
                {"addresses": ["10.244.0.16"], "conditions": {"ready": True}},
            ],
            "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}],
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "payment-gateway-slice-1",
                "labels": {"kubernetes.io/service-name": "payment-gateway"},
            },
            "addressType": "IPv4",
            "endpoints": [
                {"addresses": ["10.244.1.20"], "conditions": {"ready": True}},
                {"addresses": ["10.244.1.22"], "conditions": {"ready": False}},
            ],
            "ports": [{"name": "http", "port": 8443, "protocol": "TCP"}],
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "frontend-web-slice-1",
                "labels": {"kubernetes.io/service-name": "frontend-web"},
            },
            "addressType": "IPv4",
            "endpoints": [
                {"addresses": ["10.244.1.30"], "conditions": {"ready": False}},
            ],
            "ports": [{"name": "http", "port": 80, "protocol": "TCP"}],
        },
    ]

    # 3. Mock Services
    raw_services = [
        {
            "metadata": {
                "namespace": "production",
                "name": "api-service",
                "creationTimestamp": (now - timedelta(days=20)).isoformat(),
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.10.15",
                "selector": {"app": "api-service"},
                "ports": [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "payment-gateway",
                "creationTimestamp": (now - timedelta(days=15)).isoformat(),
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.10.22",
                "selector": {"app": "payment-gateway"},
                "ports": [{"name": "https", "port": 443, "targetPort": 8443, "protocol": "TCP"}],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "frontend-web",
                "creationTimestamp": (now - timedelta(days=10)).isoformat(),
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.10.88",
                "selector": {"app": "frontend-web"},
                "ports": [{"name": "http", "port": 80, "targetPort": 80, "protocol": "TCP"}],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "external-db",
                "creationTimestamp": (now - timedelta(days=30)).isoformat(),
            },
            "spec": {
                "type": "ExternalName",
                "clusterIP": None,
                "selector": {},
                "ports": [{"name": "postgres", "port": 5432, "targetPort": 5432, "protocol": "TCP"}],
            },
        },
    ]

    # 4. Mock Deployments
    raw_deployments = [
        {
            "metadata": {
                "namespace": "kube-system",
                "name": "coredns",
                "generation": 1,
                "creationTimestamp": (now - timedelta(days=10)).isoformat(),
            },
            "spec": {"replicas": 2},
            "status": {
                "observedGeneration": 1,
                "updatedReplicas": 2,
                "readyReplicas": 2,
                "availableReplicas": 2,
                "unavailableReplicas": 0,
                "conditions": [{"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable"}],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "api-service",
                "generation": 3,
                "creationTimestamp": (now - timedelta(days=20)).isoformat(),
            },
            "spec": {"replicas": 2},
            "status": {
                "observedGeneration": 3,
                "updatedReplicas": 2,
                "readyReplicas": 2,
                "availableReplicas": 2,
                "unavailableReplicas": 0,
                "conditions": [{"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable"}],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "payment-gateway",
                "generation": 5,
                "creationTimestamp": (now - timedelta(days=15)).isoformat(),
            },
            "spec": {"replicas": 2},
            "status": {
                "observedGeneration": 5,
                "updatedReplicas": 2,
                "readyReplicas": 1,
                "availableReplicas": 1,
                "unavailableReplicas": 1,
                "conditions": [{"type": "Progressing", "status": "True", "reason": "ReplicaSetUpdated"}],
            },
        },
        {
            "metadata": {
                "namespace": "production",
                "name": "frontend-web",
                "generation": 4,
                "creationTimestamp": (now - timedelta(days=10)).isoformat(),
            },
            "spec": {"replicas": 3},
            "status": {
                "observedGeneration": 4,
                "updatedReplicas": 3,
                "readyReplicas": 0,
                "availableReplicas": 0,
                "unavailableReplicas": 3,
                "conditions": [
                    {
                        "type": "Progressing",
                        "status": "False",
                        "reason": "ProgressDeadlineExceeded",
                        "message": "ReplicaSet 'frontend-web-66c5d9' has timed out progressing",
                    }
                ],
            },
        },
        {
            "metadata": {
                "namespace": "staging",
                "name": "batch-archiver",
                "generation": 1,
                "creationTimestamp": (now - timedelta(days=5)).isoformat(),
            },
            "spec": {"replicas": 0},
            "status": {
                "observedGeneration": 1,
                "updatedReplicas": 0,
                "readyReplicas": 0,
                "availableReplicas": 0,
                "unavailableReplicas": 0,
                "conditions": [],
            },
        },
    ]

    # 5. Mock Events
    raw_events = [
        {
            "metadata": {"namespace": "production", "name": "ev-pay-backoff-1"},
            "involvedObject": {"kind": "Pod", "name": "payment-gateway-9df8b-p98a", "namespace": "production"},
            "reason": "BackOff",
            "message": "Back-off restarting failed container payment-worker",
            "type": "Warning",
            "count": 12,
            "firstTimestamp": (now - timedelta(hours=1)).isoformat(),
            "lastTimestamp": (now - timedelta(minutes=2)).isoformat(),
        },
        {
            "metadata": {"namespace": "production", "name": "ev-front-failed-1"},
            "involvedObject": {"kind": "Pod", "name": "frontend-web-66c5d9-48kj", "namespace": "production"},
            "reason": "Failed",
            "message": "Failed to pull image 'myregistry.io/frontend:non-existent-tag': rpc error: not found",
            "type": "Warning",
            "count": 4,
            "firstTimestamp": (now - timedelta(minutes=40)).isoformat(),
            "lastTimestamp": (now - timedelta(minutes=5)).isoformat(),
        },
    ]

    return {
        "pods": raw_pods,
        "endpointslices": raw_endpointslices,
        "services": raw_services,
        "deployments": raw_deployments,
        "events": raw_events,
    }


class ClusterWatcher:
    """Orchestrates resource watchers, background reconciliation, and mock simulation."""

    def __init__(self):
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._watcher_status: Dict[str, str] = {}

    def start(self) -> None:
        """Start watchers or mock simulation loop."""
        self._stop_event.clear()

        if settings.app_mode == "mock":
            logger.info("Starting MOCK mode simulation.")
            self._sync_mock_data()
            t = threading.Thread(target=self._mock_reconciliation_loop, name="MockReconciliation", daemon=True)
            self._threads.append(t)
            t.start()
            self._watcher_status["simulation"] = "running"
            return

        if not k8s_manager.connected:
            logger.warning("Kubernetes client is not connected. Background watchers will not start.")
            self._watcher_status["all"] = "not_connected"
            return

        # Spawn initial sync & watchers in background so Uvicorn can open port immediately
        t_init = threading.Thread(
            target=self._initial_sync_and_start_watchers,
            name="InitialSyncAndWatchers",
            daemon=True,
        )
        self._threads.append(t_init)
        t_init.start()
        self._watcher_status["initial_sync"] = "running"

    def _initial_sync_and_start_watchers(self) -> None:
        """Execute initial synchronization and launch watch loops in background."""
        logger.info("Executing initial cluster synchronization in background thread.")
        try:
            self.reconcile()
        except Exception as e:
            logger.error("Initial synchronization failed: %s", e)

        # Start background threads for list/watch
        watchers = [
            ("pods", self._watch_pods),
            ("deployments", self._watch_deployments),
            ("services", self._watch_services),
            ("endpointslices", self._watch_endpointslices),
            ("events", self._watch_events),
        ]

        for name, func in watchers:
            t = threading.Thread(target=func, name=f"Watcher-{name}", daemon=True)
            self._threads.append(t)
            t.start()
            self._watcher_status[name] = "running"

        # Start periodic reconciliation loop
        t_rec = threading.Thread(target=self._periodic_reconciliation_loop, name="ReconciliationLoop", daemon=True)
        self._threads.append(t_rec)
        t_rec.start()
        self._watcher_status["reconciliation"] = "running"

    def stop(self) -> None:
        """Signal all background threads to stop and join them."""
        logger.info("Stopping Kubernetes watchers...")
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=2.0)
        self._threads.clear()
        self._watcher_status.clear()
        logger.info("Kubernetes watchers stopped.")

    def get_status(self) -> Dict[str, str]:
        return dict(self._watcher_status)

    def _sync_mock_data(self) -> None:
        """Load simulated data into the cache and evaluate alerts."""
        mock = generate_mock_cluster_data()

        # EndpointSlices first
        parsed_slices = [parse_endpointslice(s) for s in mock["endpointslices"]]
        resource_cache.set_endpointslices(parsed_slices)

        # Services
        parsed_services = [parse_service(s, parsed_slices) for s in mock["services"]]
        resource_cache.set_services(parsed_services)

        # Deployments
        parsed_deployments = [parse_deployment(d) for d in mock["deployments"]]
        resource_cache.set_deployments(parsed_deployments)

        # Pods
        parsed_pods = [
            parse_pod(
                p,
                pending_threshold_seconds=settings.pending_pod_threshold_seconds,
                crash_loop_restart_threshold=settings.crash_loop_restart_threshold,
            )
            for p in mock["pods"]
        ]
        resource_cache.set_pods(parsed_pods)

        # Events
        parsed_events = [parse_event(e) for e in mock["events"]]
        resource_cache.set_events(parsed_events)

        # Alert evaluation
        alert_manager.evaluate(parsed_pods, parsed_deployments, parsed_services)
        resource_cache.set_initial_sync_completed(True)

    def _mock_reconciliation_loop(self) -> None:
        """Periodic loop refreshing timestamps and maintaining mock simulation."""
        while not self._stop_event.is_set():
            time.sleep(min(15, settings.reconciliation_interval_seconds))
            if self._stop_event.is_set():
                break
            try:
                self._sync_mock_data()
            except Exception as e:
                logger.error("Error in mock simulation loop: %s", e)

    def reconcile(self) -> None:
        """Perform a full synchronization across all resources."""
        if not k8s_manager.connected or k8s_manager.is_mock:
            if k8s_manager.is_mock:
                self._sync_mock_data()
            return

        try:
            # 1. EndpointSlices
            if k8s_manager.discovery_v1:
                logger.info("Fetching EndpointSlices across all namespaces...")
                raw_slices = k8s_manager.discovery_v1.list_endpoint_slice_for_all_namespaces(_request_timeout=10).items
                parsed_slices = [parse_endpointslice(s) for s in raw_slices]
                resource_cache.set_endpointslices(parsed_slices)
                logger.info("Fetched %d EndpointSlices.", len(parsed_slices))
            else:
                parsed_slices = resource_cache.get_endpointslices()

            # 2. Services
            if k8s_manager.core_v1:
                logger.info("Fetching Services across all namespaces...")
                raw_services = k8s_manager.core_v1.list_service_for_all_namespaces(_request_timeout=10).items
                parsed_services = [parse_service(s, parsed_slices) for s in raw_services]
                resource_cache.set_services(parsed_services)
                logger.info("Fetched %d Services.", len(parsed_services))
            else:
                parsed_services = resource_cache.get_services()

            # 3. Deployments
            if k8s_manager.apps_v1:
                logger.info("Fetching Deployments across all namespaces...")
                raw_deps = k8s_manager.apps_v1.list_deployment_for_all_namespaces(_request_timeout=10).items
                parsed_deployments = [parse_deployment(d) for d in raw_deps]
                resource_cache.set_deployments(parsed_deployments)
                logger.info("Fetched %d Deployments.", len(parsed_deployments))
            else:
                parsed_deployments = resource_cache.get_deployments()

            # 4. Pods
            if k8s_manager.core_v1:
                logger.info("Fetching Pods across all namespaces...")
                raw_pods = k8s_manager.core_v1.list_pod_for_all_namespaces(_request_timeout=10).items
                parsed_pods = [
                    parse_pod(
                        p,
                        pending_threshold_seconds=settings.pending_pod_threshold_seconds,
                        crash_loop_restart_threshold=settings.crash_loop_restart_threshold,
                    )
                    for p in raw_pods
                ]
                resource_cache.set_pods(parsed_pods)
                logger.info("Fetched %d Pods.", len(parsed_pods))
            else:
                parsed_pods = resource_cache.get_pods()

            # 5. Events
            if k8s_manager.core_v1:
                logger.info("Fetching Events across all namespaces...")
                raw_events = k8s_manager.core_v1.list_event_for_all_namespaces(_request_timeout=10).items
                parsed_events = [parse_event(e) for e in raw_events]
                resource_cache.set_events(parsed_events)
                logger.info("Fetched %d Events.", len(parsed_events))

            # Evaluate alerts
            alert_manager.evaluate(parsed_pods, parsed_deployments, parsed_services)
            resource_cache.set_initial_sync_completed(True)
            logger.info("Cluster reconciliation completed successfully.")

        except Exception as e:
            logger.error("Error during cluster reconciliation: %s", e)
            resource_cache.record_sync_error("reconciliation", str(e))

    def _periodic_reconciliation_loop(self) -> None:
        """Periodic background reconciliation to guarantee eventual consistency."""
        while not self._stop_event.is_set():
            time.sleep(settings.reconciliation_interval_seconds)
            if self._stop_event.is_set():
                break
            self.reconcile()

    def _watch_pods(self) -> None:
        """Watch loop for Pods with automatic reconnect and 410 handling."""
        resource_version = ""
        backoff = 1

        while not self._stop_event.is_set():
            try:
                if not k8s_manager.core_v1 or watch is None:
                    time.sleep(5)
                    continue

                w = watch.Watch()
                list_call = k8s_manager.core_v1.list_pod_for_all_namespaces
                stream = (
                    w.stream(list_call, resource_version=resource_version, timeout_seconds=60)
                    if resource_version
                    else w.stream(list_call, timeout_seconds=60)
                )

                for event in stream:
                    if self._stop_event.is_set():
                        w.stop()
                        break

                    ev_type = event.get("type")
                    obj = event.get("object")
                    if obj is None:
                        continue

                    # Track resource version
                    rv = getattr(obj.metadata, "resource_version", None) if hasattr(obj, "metadata") else None
                    if rv:
                        resource_version = rv

                    if ev_type in ("ADDED", "MODIFIED"):
                        parsed = parse_pod(
                            obj,
                            pending_threshold_seconds=settings.pending_pod_threshold_seconds,
                            crash_loop_restart_threshold=settings.crash_loop_restart_threshold,
                        )
                        resource_cache.upsert_pod(parsed)
                    elif ev_type == "DELETED":
                        ns = getattr(obj.metadata, "namespace", "default")
                        name = getattr(obj.metadata, "name", "")
                        resource_cache.delete_pod(ns, name)

                    # Quick alert re-evaluation on pod change
                    alert_manager.evaluate(
                        resource_cache.get_pods(),
                        resource_cache.get_deployments(),
                        resource_cache.get_services(),
                    )
                    backoff = 1

            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.warning("Pod watch stream interrupted: %s. Reconnecting in %ss...", e, backoff)
                resource_version = ""  # Reset to relist
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _watch_deployments(self) -> None:
        """Watch loop for Deployments."""
        resource_version = ""
        backoff = 1

        while not self._stop_event.is_set():
            try:
                if not k8s_manager.apps_v1 or watch is None:
                    time.sleep(5)
                    continue

                w = watch.Watch()
                list_call = k8s_manager.apps_v1.list_deployment_for_all_namespaces
                stream = (
                    w.stream(list_call, resource_version=resource_version, timeout_seconds=60)
                    if resource_version
                    else w.stream(list_call, timeout_seconds=60)
                )

                for event in stream:
                    if self._stop_event.is_set():
                        w.stop()
                        break

                    ev_type = event.get("type")
                    obj = event.get("object")
                    if obj is None:
                        continue

                    rv = getattr(obj.metadata, "resource_version", None) if hasattr(obj, "metadata") else None
                    if rv:
                        resource_version = rv

                    if ev_type in ("ADDED", "MODIFIED"):
                        parsed = parse_deployment(obj)
                        resource_cache.upsert_deployment(parsed)
                    elif ev_type == "DELETED":
                        ns = getattr(obj.metadata, "namespace", "default")
                        name = getattr(obj.metadata, "name", "")
                        resource_cache.delete_deployment(ns, name)

                    alert_manager.evaluate(
                        resource_cache.get_pods(),
                        resource_cache.get_deployments(),
                        resource_cache.get_services(),
                    )
                    backoff = 1

            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.warning("Deployment watch interrupted: %s. Reconnecting in %ss...", e, backoff)
                resource_version = ""
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _watch_services(self) -> None:
        """Watch loop for Services."""
        resource_version = ""
        backoff = 1

        while not self._stop_event.is_set():
            try:
                if not k8s_manager.core_v1 or watch is None:
                    time.sleep(5)
                    continue

                w = watch.Watch()
                list_call = k8s_manager.core_v1.list_service_for_all_namespaces
                stream = (
                    w.stream(list_call, resource_version=resource_version, timeout_seconds=60)
                    if resource_version
                    else w.stream(list_call, timeout_seconds=60)
                )

                for event in stream:
                    if self._stop_event.is_set():
                        w.stop()
                        break

                    ev_type = event.get("type")
                    obj = event.get("object")
                    if obj is None:
                        continue

                    rv = getattr(obj.metadata, "resource_version", None) if hasattr(obj, "metadata") else None
                    if rv:
                        resource_version = rv

                    if ev_type in ("ADDED", "MODIFIED"):
                        slices = resource_cache.get_endpointslices()
                        parsed = parse_service(obj, slices)
                        resource_cache.upsert_service(parsed)
                    elif ev_type == "DELETED":
                        ns = getattr(obj.metadata, "namespace", "default")
                        name = getattr(obj.metadata, "name", "")
                        resource_cache.delete_service(ns, name)

                    alert_manager.evaluate(
                        resource_cache.get_pods(),
                        resource_cache.get_deployments(),
                        resource_cache.get_services(),
                    )
                    backoff = 1

            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.warning("Service watch interrupted: %s. Reconnecting in %ss...", e, backoff)
                resource_version = ""
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _watch_endpointslices(self) -> None:
        """Watch loop for EndpointSlices."""
        resource_version = ""
        backoff = 1

        while not self._stop_event.is_set():
            try:
                if not k8s_manager.discovery_v1 or watch is None:
                    time.sleep(5)
                    continue

                w = watch.Watch()
                list_call = k8s_manager.discovery_v1.list_endpoint_slice_for_all_namespaces
                stream = (
                    w.stream(list_call, resource_version=resource_version, timeout_seconds=60)
                    if resource_version
                    else w.stream(list_call, timeout_seconds=60)
                )

                for event in stream:
                    if self._stop_event.is_set():
                        w.stop()
                        break

                    ev_type = event.get("type")
                    obj = event.get("object")
                    if obj is None:
                        continue

                    rv = getattr(obj.metadata, "resource_version", None) if hasattr(obj, "metadata") else None
                    if rv:
                        resource_version = rv

                    if ev_type in ("ADDED", "MODIFIED"):
                        parsed = parse_endpointslice(obj)
                        resource_cache.upsert_endpointslice(parsed)
                    elif ev_type == "DELETED":
                        ns = getattr(obj.metadata, "namespace", "default")
                        name = getattr(obj.metadata, "name", "")
                        resource_cache.delete_endpointslice(ns, name)

                    # Update associated services when endpoints change
                    raw_services = resource_cache.get_services()
                    slices = resource_cache.get_endpointslices()
                    for s in raw_services:
                        updated_s = parse_service(s, slices)
                        resource_cache.upsert_service(updated_s)

                    alert_manager.evaluate(
                        resource_cache.get_pods(),
                        resource_cache.get_deployments(),
                        resource_cache.get_services(),
                    )
                    backoff = 1

            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.warning("EndpointSlice watch interrupted: %s. Reconnecting in %ss...", e, backoff)
                resource_version = ""
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _watch_events(self) -> None:
        """Watch loop for Kubernetes Events."""
        resource_version = ""
        backoff = 1

        while not self._stop_event.is_set():
            try:
                if not k8s_manager.core_v1 or watch is None:
                    time.sleep(5)
                    continue

                w = watch.Watch()
                list_call = k8s_manager.core_v1.list_event_for_all_namespaces
                stream = (
                    w.stream(list_call, resource_version=resource_version, timeout_seconds=60)
                    if resource_version
                    else w.stream(list_call, timeout_seconds=60)
                )

                for event in stream:
                    if self._stop_event.is_set():
                        w.stop()
                        break

                    ev_type = event.get("type")
                    obj = event.get("object")
                    if obj is None:
                        continue

                    rv = getattr(obj.metadata, "resource_version", None) if hasattr(obj, "metadata") else None
                    if rv:
                        resource_version = rv

                    if ev_type in ("ADDED", "MODIFIED"):
                        parsed = parse_event(obj)
                        resource_cache.add_event(parsed)
                    backoff = 1

            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.warning("Event watch interrupted: %s. Reconnecting in %ss...", e, backoff)
                resource_version = ""
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)


# Global cluster watcher instance
cluster_watcher = ClusterWatcher()
