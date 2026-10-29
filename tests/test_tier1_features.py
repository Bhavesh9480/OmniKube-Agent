"""Unit tests for Tier 1 lightweight enhancements:

1. Native Metrics Server parser (CPU millicores & Memory bytes).
2. Auto-fetch crash logs on failure.
3. Lightweight native webhook alerts (Slack & Discord payload builders).
4. Alert anti-flapping protection and cooldown logic.
"""

import time
from app.alerts import AlertManager, fetch_crash_log_snippet
from app.collectors.metrics import (
    format_memory_bytes,
    parse_cpu_quantity,
    parse_memory_quantity,
    parse_pod_metrics,
)
from app.collectors.pods import parse_pod
from app.config import settings
from app.kubernetes_client import k8s_manager
from app.notifications import (
    build_discord_payload,
    build_generic_payload,
    build_slack_payload,
    dispatch_webhook,
)
from tests.fixtures.k8s_mocks import make_mock_pod


# --- Feature 1: Metrics Server Parsers ---

def test_parse_cpu_quantity():
    """Verify conversion of various Kubernetes CPU quantity formats to millicores."""
    assert parse_cpu_quantity("100m") == 100
    assert parse_cpu_quantity("1") == 1000
    assert parse_cpu_quantity("2.5") == 2500
    assert parse_cpu_quantity("50000000n") == 50  # 50 million nanocores = 50 millicores
    assert parse_cpu_quantity("50000u") == 50      # 50,000 microcores = 50 millicores
    assert parse_cpu_quantity("") == 0
    assert parse_cpu_quantity(None) == 0


def test_parse_memory_quantity():
    """Verify conversion of binary and decimal SI memory units to bytes."""
    assert parse_memory_quantity("128Ki") == 128 * 1024
    assert parse_memory_quantity("64Mi") == 64 * 1024**2
    assert parse_memory_quantity("1Gi") == 1 * 1024**3
    assert parse_memory_quantity("100M") == 100 * 1000**2
    assert parse_memory_quantity("500k") == 500 * 1000
    assert parse_memory_quantity("1048576") == 1048576
    assert parse_memory_quantity("") == 0
    assert parse_memory_quantity(None) == 0


def test_format_memory_bytes():
    """Verify human-friendly memory formatting."""
    assert "KiB" in format_memory_bytes(100 * 1024)
    assert "MiB" in format_memory_bytes(64 * 1024**2)
    assert "GiB" in format_memory_bytes(2 * 1024**3)


def test_parse_pod_metrics():
    """Verify parsing raw metrics.k8s.io PodMetrics items into structured metrics lookup map."""
    raw_items = [
        {
            "metadata": {"namespace": "prod", "name": "web-pod-1"},
            "timestamp": "2026-10-28T10:00:00Z",
            "containers": [
                {"name": "nginx", "usage": {"cpu": "25m", "memory": "32Mi"}},
                {"name": "sidecar", "usage": {"cpu": "10000000n", "memory": "16Mi"}},
            ],
        }
    ]
    parsed = parse_pod_metrics(raw_items)
    assert "prod/web-pod-1" in parsed
    pod_m = parsed["prod/web-pod-1"]
    assert pod_m["cpuMillicores"] == 35  # 25m + 10m
    assert pod_m["memoryBytes"] == (32 + 16) * 1024**2
    assert len(pod_m["containers"]) == 2
    assert pod_m["containers"][0]["cpuMillicores"] == 25


# --- Feature 2: Crash Log Snippet Capture ---

def test_crash_log_capture_in_mock_mode():
    """Verify crash log snippet is auto-captured for CrashLoopBackOff pods in mock mode."""
    k8s_manager.is_mock = True
    settings.enable_crash_logs = True

    crashed_pod = parse_pod(
        make_mock_pod(
            namespace="production",
            name="auth-service",
            phase="Running",
            ready=False,
            containers=[
                {
                    "name": "auth",
                    "image": "img",
                    "ready": False,
                    "restartCount": 4,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                }
            ],
        )
    )

    mgr = AlertManager()
    mgr.evaluate(pods=[crashed_pod], deployments=[], services=[])

    alerts = mgr.get_alerts()
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["rule"] if "rule" in alert else alert["reason"] == "CrashLoopBackOff"
    assert alert["lastLogSnippet"] is not None
    assert "Database connection failed" in alert["lastLogSnippet"]


# --- Feature 3: Webhook Payload Builders ---

def test_slack_webhook_payload():
    """Verify Slack payload builder produces valid attachment structure."""
    alert = {
        "id": "abc123",
        "severity": "critical",
        "title": "Pod web-1 in CrashLoopBackOff",
        "resourceKind": "Pod",
        "resourceName": "web-1",
        "namespace": "default",
        "reason": "CrashLoopBackOff",
        "message": "Back-off restarting failed container",
        "lastLogSnippet": "FATAL: Out of memory",
        "status": "active",
    }
    payload = build_slack_payload("alert_created", alert)
    assert "text" in payload
    assert "attachments" in payload
    assert len(payload["attachments"]) == 1
    att = payload["attachments"][0]
    assert att["color"] == "#d73a49"
    field_titles = [f["title"] for f in att["fields"]]
    assert "Resource" in field_titles
    assert "Crash Log Snippet" in field_titles


def test_discord_webhook_payload():
    """Verify Discord payload builder produces valid embed structure."""
    alert = {
        "id": "def456",
        "severity": "warning",
        "title": "Deployment api replica shortage",
        "resourceKind": "Deployment",
        "resourceName": "api",
        "namespace": "prod",
        "message": "1/3 replicas available",
        "status": "active",
    }
    payload = build_discord_payload("alert_created", alert)
    assert "embeds" in payload
    assert len(payload["embeds"]) == 1
    embed = payload["embeds"][0]
    assert embed["color"] == 0xF66A0A


def test_dispatch_webhook_handles_empty_url_gracefully():
    """Verify dispatch_webhook does not fail or hang if webhook URL is None or empty."""
    alert = {"title": "test"}
    # Should return immediately with 0 exceptions
    dispatch_webhook("alert_created", alert, webhook_url=None)
    dispatch_webhook("alert_created", alert, webhook_url="")


# --- Feature 4: Alert Anti-Flapping Protection ---

def test_alert_anti_flapping_cooldown():
    """Verify alert does not immediately resolve if flapping cooldown is configured."""
    # Configure 5 seconds cooldown
    mgr = AlertManager(flapping_cooldown_seconds=5)

    bad_pod = parse_pod(
        make_mock_pod(
            name="flapping-worker",
            phase="Running",
            ready=False,
            containers=[
                {
                    "name": "worker",
                    "image": "img",
                    "ready": False,
                    "restartCount": 5,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                }
            ],
        )
    )

    # 1. Trigger alert
    mgr.evaluate(pods=[bad_pod], deployments=[], services=[])
    active = mgr.get_alerts(include_resolved=False)
    assert len(active) == 1
    assert active[0].get("pendingResolve") is None or active[0].get("pendingResolve") is False

    # 2. Pod temporarily recovers
    good_pod = parse_pod(
        make_mock_pod(
            name="flapping-worker",
            phase="Running",
            ready=True,
            containers=[{"name": "worker", "image": "img", "ready": True, "restartCount": 0, "state": {"running": {}}}],
        )
    )

    mgr.evaluate(pods=[good_pod], deployments=[], services=[])
    # Anti-flapping: Alert should STILL be active, flagged with pendingResolve!
    active_during_cooldown = mgr.get_alerts(include_resolved=False)
    assert len(active_during_cooldown) == 1
    assert active_during_cooldown[0].get("pendingResolve") is True

    # 3. Flapping: Pod crashes AGAIN before cooldown expires!
    mgr.evaluate(pods=[bad_pod], deployments=[], services=[])
    # Flapping recovery: pendingResolve should be cleared and alert remains active!
    active_after_flap = mgr.get_alerts(include_resolved=False)
    assert len(active_after_flap) == 1
    assert active_after_flap[0].get("pendingResolve") is False or active_after_flap[0].get("pendingResolve") is None

    # 4. Now pod recovers and we simulate time passing past cooldown window
    mgr.evaluate(pods=[good_pod], deployments=[], services=[])
    # Manually shift pending_resolutions timestamp back by 10 seconds to simulate time passage
    alert_id = active_after_flap[0]["id"]
    mgr._pending_resolutions[alert_id] = time.time() - 10

    # Next sweep: cooldown expired, alert should now resolve!
    mgr.evaluate(pods=[good_pod], deployments=[], services=[])
    assert len(mgr.get_alerts(include_resolved=False)) == 0
    resolved = mgr.get_alerts(include_resolved=True)
    assert len(resolved) == 1
    assert resolved[0]["status"] == "resolved"
