"""Unit tests for Kubernetes Pod collector and health analysis."""

from datetime import datetime, timezone, timedelta
from app.collectors.pods import parse_pod
from tests.fixtures.k8s_mocks import make_mock_pod


def test_healthy_running_pod():
    pod_data = make_mock_pod(
        name="web-app",
        phase="Running",
        ready=True,
        containers=[
            {"name": "web", "image": "nginx:latest", "ready": True, "restartCount": 0, "state": {"running": {}}}
        ],
    )
    parsed = parse_pod(pod_data)

    assert parsed["name"] == "web-app"
    assert parsed["phase"] == "Running"
    assert parsed["ready"] is True
    assert parsed["readyContainers"] == 1
    assert parsed["totalContainers"] == 1
    assert parsed["restarts"] == 0
    assert parsed["isUnhealthy"] is False
    assert parsed["isTerminating"] is False
    assert len(parsed["issues"]) == 0


def test_running_pod_with_unready_container():
    """Do not assume a Running pod is healthy merely because its phase is Running."""
    pod_data = make_mock_pod(
        name="api-app",
        phase="Running",
        ready=False,
        containers=[
            {"name": "c1", "image": "app:1.0", "ready": True, "restartCount": 0, "state": {"running": {}}},
            {"name": "c2", "image": "sidecar:1.0", "ready": False, "restartCount": 0, "state": {"running": {}}},
        ],
    )
    parsed = parse_pod(pod_data)

    assert parsed["phase"] == "Running"
    assert parsed["ready"] is False
    assert parsed["readyContainers"] == 1
    assert parsed["totalContainers"] == 2
    assert parsed["isUnhealthy"] is True
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "ContainersNotReady" in issue_types


def test_pod_crash_loop_back_off():
    pod_data = make_mock_pod(
        name="crashing-app",
        phase="Running",
        ready=False,
        containers=[
            {
                "name": "worker",
                "image": "worker:latest",
                "ready": False,
                "restartCount": 5,
                "state": {
                    "waiting": {
                        "reason": "CrashLoopBackOff",
                        "message": "Back-off 5m0s restarting failed container",
                    }
                },
            }
        ],
    )
    parsed = parse_pod(pod_data, crash_loop_restart_threshold=3)

    assert parsed["isUnhealthy"] is True
    assert parsed["restarts"] == 5
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "CrashLoopBackOff" in issue_types
    assert "HighRestartCount" in issue_types


def test_pod_image_pull_failure():
    pod_data = make_mock_pod(
        name="broken-image-app",
        phase="Pending",
        ready=False,
        containers=[
            {
                "name": "app",
                "image": "myrepo/nonexistent:tag",
                "ready": False,
                "restartCount": 0,
                "state": {
                    "waiting": {
                        "reason": "ImagePullBackOff",
                        "message": "Back-off pulling image",
                    }
                },
            }
        ],
    )
    parsed = parse_pod(pod_data)

    assert parsed["isUnhealthy"] is True
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "ImagePullBackOff" in issue_types


def test_pod_create_container_config_error():
    pod_data = make_mock_pod(
        name="config-error-pod",
        phase="Pending",
        ready=False,
        containers=[
            {
                "name": "db-client",
                "image": "client:v1",
                "ready": False,
                "restartCount": 0,
                "state": {
                    "waiting": {
                        "reason": "CreateContainerConfigError",
                        "message": "secret 'db-secret' not found",
                    }
                },
            }
        ],
    )
    parsed = parse_pod(pod_data)

    assert parsed["isUnhealthy"] is True
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "CreateContainerConfigError" in issue_types


def test_pod_prolonged_pending():
    # Pod created 10 minutes ago
    created_old = datetime.now(timezone.utc) - timedelta(minutes=10)
    pod_old = make_mock_pod(name="stuck-pending", phase="Pending", ready=False, created_at=created_old)
    parsed_old = parse_pod(pod_old, pending_threshold_seconds=300)

    issue_types_old = [i["type"] for i in parsed_old["issues"]]
    assert "ProlongedPending" in issue_types_old
    assert parsed_old["isUnhealthy"] is True

    # Pod created 1 minute ago (under threshold)
    created_new = datetime.now(timezone.utc) - timedelta(seconds=60)
    pod_new = make_mock_pod(name="new-pending", phase="Pending", ready=False, created_at=created_new)
    parsed_new = parse_pod(pod_new, pending_threshold_seconds=300)

    issue_types_new = [i["type"] for i in parsed_new["issues"]]
    assert "PodPending" in issue_types_new
    assert "ProlongedPending" not in issue_types_new


def test_pod_failed_phase():
    pod_data = make_mock_pod(
        name="failed-job-pod",
        phase="Failed",
        ready=False,
        reason="DeadlineExceeded",
        message="Pod was active on the node longer than the specified deadline",
    )
    parsed = parse_pod(pod_data)

    assert parsed["phase"] == "Failed"
    assert parsed["isUnhealthy"] is True
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "PodFailed" in issue_types


def test_pod_terminating_stuck():
    deleted_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    pod_data = make_mock_pod(name="terminating-pod", phase="Running", deletion_timestamp=deleted_at)
    parsed = parse_pod(pod_data)

    assert parsed["isTerminating"] is True
    assert parsed["isUnhealthy"] is True
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "PodTerminating" in issue_types


def test_container_restart_aggregation():
    pod_data = make_mock_pod(
        name="multi-container-restarts",
        phase="Running",
        containers=[
            {"name": "c1", "image": "i1", "ready": True, "restartCount": 2, "state": {"running": {}}},
            {"name": "c2", "image": "i2", "ready": True, "restartCount": 3, "state": {"running": {}}},
            {"name": "c3", "image": "i3", "ready": True, "restartCount": 1, "state": {"running": {}}},
        ],
    )
    parsed = parse_pod(pod_data)
    assert parsed["restarts"] == 6
    assert parsed["totalContainers"] == 3
