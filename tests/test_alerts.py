"""Unit tests for AlertManager rule evaluation, deduplication, and resolution."""

from app.alerts import AlertManager, generate_alert_id
from app.collectors.deployments import parse_deployment
from app.collectors.endpointslices import parse_endpointslice
from app.collectors.pods import parse_pod
from app.collectors.services import parse_service
from tests.fixtures.k8s_mocks import (
    make_mock_deployment,
    make_mock_endpointslice,
    make_mock_pod,
    make_mock_service,
)


def test_deterministic_alert_id():
    id1 = generate_alert_id("PodCrashLoop", "Pod", "default", "my-pod")
    id2 = generate_alert_id("PodCrashLoop", "Pod", "default", "my-pod")
    id3 = generate_alert_id("PodCrashLoop", "Pod", "production", "my-pod")

    assert id1 == id2
    assert id1 != id3
    assert len(id1) == 16


def test_alert_generation_and_deduplication():
    mgr = AlertManager()

    # Pod with CrashLoopBackOff
    pod_data = make_mock_pod(
        namespace="prod",
        name="api-worker",
        phase="Running",
        ready=False,
        containers=[
            {
                "name": "worker",
                "image": "img",
                "ready": False,
                "restartCount": 5,
                "state": {"waiting": {"reason": "CrashLoopBackOff", "message": "CrashLoop"}},
            }
        ],
    )
    parsed_pod = parse_pod(pod_data)

    # First evaluation sweep
    mgr.evaluate(pods=[parsed_pod], deployments=[], services=[])
    alerts = mgr.get_alerts()
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["severity"] == "critical"
    assert alert["status"] == "active"
    assert alert["resourceName"] == "api-worker"
    assert alert["namespace"] == "prod"
    first_seen = alert["firstSeen"]

    # Second evaluation sweep (problem still exists)
    mgr.evaluate(pods=[parsed_pod], deployments=[], services=[])
    alerts_after = mgr.get_alerts()
    # Suppress duplicate alerts: count should still be 1!
    assert len(alerts_after) == 1
    assert alerts_after[0]["firstSeen"] == first_seen


def test_alert_auto_resolution():
    mgr = AlertManager()

    # Create unhealthy pod
    bad_pod = parse_pod(
        make_mock_pod(
            name="flapping-pod",
            phase="Running",
            ready=False,
            containers=[
                {
                    "name": "app",
                    "image": "img",
                    "ready": False,
                    "restartCount": 4,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                }
            ],
        )
    )

    mgr.evaluate(pods=[bad_pod], deployments=[], services=[])
    active_alerts = mgr.get_alerts(include_resolved=False)
    assert len(active_alerts) == 1
    alert_id = active_alerts[0]["id"]

    # Now the pod recovers and becomes healthy
    recovered_pod = parse_pod(
        make_mock_pod(
            name="flapping-pod",
            phase="Running",
            ready=True,
            containers=[{"name": "app", "image": "img", "ready": True, "restartCount": 0, "state": {"running": {}}}],
        )
    )

    # Next evaluation sweep
    mgr.evaluate(pods=[recovered_pod], deployments=[], services=[])

    # Active alerts should be 0, resolved alert present in history
    assert len(mgr.get_alerts(include_resolved=False)) == 0
    all_alerts = mgr.get_alerts(include_resolved=True)
    assert len(all_alerts) == 1
    assert all_alerts[0]["id"] == alert_id
    assert all_alerts[0]["status"] == "resolved"


def test_service_and_deployment_alerts():
    mgr = AlertManager()

    # Degraded deployment
    dep = parse_deployment(make_mock_deployment(name="bad-dep", desired=3, ready=1, available=1, unavailable=2))

    # Service without endpoints
    ep_slice = parse_endpointslice(make_mock_endpointslice(name="s-slice", service_name="bad-svc", endpoints=[]))
    svc = parse_service(make_mock_service(name="bad-svc", selector={"app": "bad"}), [ep_slice])

    mgr.evaluate(pods=[], deployments=[dep], services=[svc])
    alerts = mgr.get_alerts()

    kinds = {a["resourceKind"] for a in alerts}
    assert "Deployment" in kinds
    assert "Service" in kinds

    counts = mgr.get_counts()
    assert counts["warning"] >= 2
    assert counts["total"] >= 2


def test_bounded_resolved_history():
    mgr = AlertManager(history_limit=5)

    # Generate and resolve 10 alerts
    for i in range(10):
        bad_pod = parse_pod(
            make_mock_pod(
                name=f"temp-pod-{i}",
                phase="Running",
                ready=False,
                containers=[
                    {
                        "name": "c",
                        "image": "img",
                        "ready": False,
                        "restartCount": 1,
                        "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    }
                ],
            )
        )
        mgr.evaluate(pods=[bad_pod], deployments=[], services=[])
        # Resolve immediately
        mgr.evaluate(pods=[], deployments=[], services=[])

    # History should be bounded to 5
    all_alerts = mgr.get_alerts(include_resolved=True)
    assert len(all_alerts) <= 5
