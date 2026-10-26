"""Integration and unit tests for FastAPI REST API endpoints."""

import pytest
from fastapi.testclient import TestClient

from app.alerts import alert_manager
from app.cache import resource_cache
from app.collectors.deployments import parse_deployment
from app.collectors.endpointslices import parse_endpointslice
from app.collectors.pods import parse_pod
from app.collectors.services import parse_service
from app.config import settings
from app.kubernetes_client import k8s_manager
from app.main import app
from tests.fixtures.k8s_mocks import (
    make_mock_deployment,
    make_mock_endpointslice,
    make_mock_pod,
    make_mock_service,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global cache, alerts, and settings before each test."""
    resource_cache.clear()
    alert_manager.clear()
    k8s_manager.connected = False
    k8s_manager.is_mock = False
    k8s_manager.last_error = None
    settings.app_mode = "production"
    yield
    resource_cache.clear()
    alert_manager.clear()


client = TestClient(app, raise_server_exceptions=False)


def test_healthz_liveness_endpoint():
    """Liveness probe must return 200 even if k8s is not connected."""
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


def test_readyz_not_ready_when_disconnected():
    """Readiness probe must return 503 when disconnected in production mode."""
    k8s_manager.connected = False
    settings.app_mode = "production"
    response = client.get("/readyz")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "not_ready"
    assert "not connected" in data["reason"].lower()


def test_readyz_ready_when_synced():
    """Readiness probe must return 200 when connected, synced, and fresh."""
    k8s_manager.connected = True
    resource_cache.set_initial_sync_completed(True)
    resource_cache.set_pods([])  # Updates last_sync_time
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_api_health_diagnostics():
    """Detailed health endpoint must return telemetry without secrets."""
    k8s_manager.connected = False
    k8s_manager.last_error = "In-cluster auth unavailable"
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "unhealthy"
    assert data["kubernetes"]["connected"] is False
    assert data["kubernetes"]["lastError"] == "In-cluster auth unavailable"
    assert "agentVersion" in data
    assert "cache" in data
    assert "watchers" in data


def test_api_summary_schema_and_counters():
    """Verify exact schema of GET /api/summary."""
    # Populate cache with 1 healthy pod, 1 crashing pod, 1 degraded deployment, 1 service without endpoints
    healthy_pod = parse_pod(make_mock_pod(name="p-healthy", phase="Running", ready=True))
    crashing_pod = parse_pod(
        make_mock_pod(
            name="p-crash",
            phase="Running",
            ready=False,
            containers=[
                {
                    "name": "c",
                    "image": "img",
                    "ready": False,
                    "restartCount": 4,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                }
            ],
        )
    )
    resource_cache.set_pods([healthy_pod, crashing_pod])

    dep_degraded = parse_deployment(
        make_mock_deployment(name="d-degraded", desired=3, ready=1, available=1, unavailable=2)
    )
    resource_cache.set_deployments([dep_degraded])

    ep_slice = parse_endpointslice(make_mock_endpointslice(name="s-slice", service_name="s-empty", endpoints=[]))
    resource_cache.set_endpointslices([ep_slice])

    svc = parse_service(make_mock_service(name="s-empty", selector={"app": "test"}), [ep_slice])
    resource_cache.set_services([svc])

    # Evaluate alerts
    alert_manager.evaluate([healthy_pod, crashing_pod], [dep_degraded], [svc])
    k8s_manager.connected = True
    resource_cache.set_initial_sync_completed(True)

    response = client.get("/api/summary")
    assert response.status_code == 200
    data = response.json()

    # Exact contract checks
    assert data["status"] in ("critical", "warning")
    assert data["cluster"]["connected"] is True
    assert data["cluster"]["lastSuccessfulSync"] is not None

    # Pod counts
    assert data["pods"]["total"] == 2
    assert data["pods"]["running"] == 2
    assert data["pods"]["unhealthy"] == 1

    # Deployment counts
    assert data["deployments"]["total"] == 1
    assert data["deployments"]["unhealthy"] == 1
    assert data["deployments"]["healthy"] == 0

    # Service counts
    assert data["services"]["total"] == 1
    assert data["services"]["withoutReadyEndpoints"] == 1

    # Alert counts
    assert data["alerts"]["total"] >= 2
    assert data["alerts"]["critical"] >= 1  # CrashLoopBackOff is critical


def test_api_pods_filtering_and_pagination():
    """Test GET /api/pods filters: namespace, phase, attention, limit, continue_token."""
    pod1 = parse_pod(make_mock_pod(namespace="default", name="pod-1", phase="Running", ready=True))
    pod2 = parse_pod(
        make_mock_pod(
            namespace="kube-system",
            name="pod-2",
            phase="Running",
            ready=False,
            containers=[{"name": "c", "image": "i", "ready": False, "restartCount": 0, "state": {"running": {}}}],
        )
    )
    pod3 = parse_pod(make_mock_pod(namespace="default", name="pod-3", phase="Pending", ready=False))
    resource_cache.set_pods([pod1, pod2, pod3])

    # Filter namespace
    r_ns = client.get("/api/pods?namespace=kube-system")
    assert r_ns.status_code == 200
    items = r_ns.json()["items"]
    assert len(items) == 1
    assert items[0]["name"] == "pod-2"

    # Filter phase
    r_phase = client.get("/api/pods?phase=pending")
    assert r_phase.status_code == 200
    items_phase = r_phase.json()["items"]
    assert len(items_phase) == 1
    assert items_phase[0]["name"] == "pod-3"

    # Filter attention
    r_att = client.get("/api/pods?attention=true")
    assert r_att.status_code == 200
    items_att = r_att.json()["items"]
    att_names = [p["name"] for p in items_att]
    assert "pod-2" in att_names
    assert "pod-3" in att_names
    assert "pod-1" not in att_names

    # Pagination with limit and continue_token
    r_page1 = client.get("/api/pods?limit=2")
    assert r_page1.status_code == 200
    data_page1 = r_page1.json()
    assert len(data_page1["items"]) == 2
    next_token = data_page1["continueToken"]
    assert next_token is not None

    r_page2 = client.get(f"/api/pods?limit=2&continue_token={next_token}")
    assert r_page2.status_code == 200
    data_page2 = r_page2.json()
    assert len(data_page2["items"]) == 1


def test_api_deployments_filtering():
    """Test GET /api/deployments with namespace and attention filters."""
    dep_healthy = parse_deployment(make_mock_deployment(namespace="ns1", name="d-good", desired=2, ready=2, available=2))
    dep_bad = parse_deployment(make_mock_deployment(namespace="ns2", name="d-bad", desired=3, ready=1, available=1, unavailable=2))
    resource_cache.set_deployments([dep_healthy, dep_bad])

    # Namespace filter
    r_ns = client.get("/api/deployments?namespace=ns1")
    assert r_ns.status_code == 200
    assert len(r_ns.json()["items"]) == 1
    assert r_ns.json()["items"][0]["name"] == "d-good"

    # Attention filter
    r_att = client.get("/api/deployments?attention=true")
    assert r_att.status_code == 200
    assert len(r_att.json()["items"]) == 1
    assert r_att.json()["items"][0]["name"] == "d-bad"


def test_api_alerts_filtering():
    """Test GET /api/alerts with severity, namespace, and limit filters."""
    pod = parse_pod(
        make_mock_pod(
            namespace="prod",
            name="worker",
            phase="Running",
            ready=False,
            containers=[
                {
                    "name": "c",
                    "image": "img",
                    "ready": False,
                    "restartCount": 5,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                }
            ],
        )
    )
    dep = parse_deployment(make_mock_deployment(namespace="prod", name="api-dep", desired=3, ready=1, available=1, unavailable=2))
    alert_manager.evaluate([pod], [dep], [])

    # Filter severity
    r_crit = client.get("/api/alerts?severity=critical")
    assert r_crit.status_code == 200
    for a in r_crit.json()["items"]:
        assert a["severity"] == "critical"

    # Filter namespace
    r_ns = client.get("/api/alerts?namespace=prod")
    assert r_ns.status_code == 200
    assert len(r_ns.json()["items"]) >= 2

    # Filter limit
    r_lim = client.get("/api/alerts?limit=1")
    assert r_lim.status_code == 200
    assert len(r_lim.json()["items"]) == 1


def test_mock_mode_simulation():
    """Verify explicit mock mode loads simulated data cleanly and sets indicators."""
    from app.watchers import cluster_watcher
    settings.app_mode = "mock"
    cluster_watcher._sync_mock_data()

    response = client.get("/api/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["cluster"]["connected"] is True
    assert data["pods"]["total"] > 0
    assert data["deployments"]["total"] > 0
    assert data["services"]["total"] > 0

    # Check health indicates mock mode
    r_health = client.get("/api/health")
    assert r_health.status_code == 200
    assert r_health.json()["isMock"] is True
    assert r_health.json()["mode"] == "mock"


def test_api_services_and_endpointslices_endpoints():
    """Verify GET /api/services and GET /api/endpointslices."""
    ep_slice = parse_endpointslice(make_mock_endpointslice(name="test-slice", service_name="test-svc", endpoints=[]))
    resource_cache.set_endpointslices([ep_slice])

    svc = parse_service(make_mock_service(name="test-svc", selector={"app": "test"}), [ep_slice])
    resource_cache.set_services([svc])

    # Query services
    r_svc = client.get("/api/services")
    assert r_svc.status_code == 200
    svc_data = r_svc.json()
    assert svc_data["count"] >= 1
    assert svc_data["items"][0]["name"] == "test-svc"
    assert svc_data["items"][0]["withoutReadyEndpoints"] is True

    # Query endpointslices
    r_slices = client.get("/api/endpointslices")
    assert r_slices.status_code == 200
    slice_data = r_slices.json()
    assert slice_data["count"] >= 1
    assert slice_data["items"][0]["name"] == "test-slice"


def test_api_events_and_metrics_endpoints():
    """Verify GET /api/events and GET /api/metrics/status."""
    from app.collectors.events import parse_event

    raw_event = {
        "metadata": {"namespace": "default", "name": "ev-1"},
        "involvedObject": {"kind": "Pod", "name": "worker", "namespace": "default"},
        "reason": "BackOff",
        "message": "Backoff restarting",
        "type": "Warning",
        "count": 2,
    }
    ev = parse_event(raw_event)
    resource_cache.set_events([ev])

    r_events = client.get("/api/events")
    assert r_events.status_code == 200
    ev_data = r_events.json()
    assert ev_data["count"] >= 1
    assert ev_data["items"][0]["reason"] == "BackOff"

    # Metrics status
    r_metrics = client.get("/api/metrics/status")
    assert r_metrics.status_code == 200
    m_data = r_metrics.json()
    assert m_data["configured"] is False
    assert "not configured" in m_data["message"].lower()


def test_websocket_stream_snapshot_and_ping():
    """Verify WebSocket /api/ws connects, receives snapshot, and responds to ping."""
    import json
    with client.websocket_connect("/api/ws") as ws:
        # First message is snapshot
        raw_msg = ws.receive_text()
        msg = json.loads(raw_msg)
        assert msg["type"] == "snapshot"
        assert "data" in msg
        assert "pods" in msg["data"]
        assert "deployments" in msg["data"]

        # Send ping
        ws.send_text("ping")
        pong_msg = ws.receive_text()
        assert json.loads(pong_msg)["type"] == "pong"

