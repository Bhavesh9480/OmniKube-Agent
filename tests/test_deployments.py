"""Unit tests for Kubernetes Deployment collector and replica analysis."""

from app.collectors.deployments import parse_deployment
from tests.fixtures.k8s_mocks import make_mock_deployment


def test_healthy_deployment():
    dep = make_mock_deployment(name="web-dep", desired=3, updated=3, ready=3, available=3, unavailable=0)
    parsed = parse_deployment(dep)

    assert parsed["name"] == "web-dep"
    assert parsed["isHealthy"] is True
    assert parsed["status"] == "healthy"
    assert parsed["desired"] == 3
    assert parsed["available"] == 3
    assert parsed["unavailable"] == 0
    assert len(parsed["issues"]) == 0


def test_replica_shortage_deployment():
    dep = make_mock_deployment(
        name="degraded-dep",
        desired=4,
        updated=4,
        ready=2,
        available=2,
        unavailable=2,
    )
    parsed = parse_deployment(dep)

    assert parsed["isHealthy"] is False
    assert parsed["status"] == "unhealthy"
    assert parsed["unavailable"] == 2
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "UnavailableReplicas" in issue_types
    assert "ReplicaShortage" in issue_types


def test_zero_replica_deployment_is_healthy():
    """Handle zero-replica deployments correctly: scaled to zero is healthy, not a shortage."""
    dep = make_mock_deployment(
        name="scaled-zero-dep",
        desired=0,
        updated=0,
        ready=0,
        available=0,
        unavailable=0,
    )
    parsed = parse_deployment(dep)

    assert parsed["isHealthy"] is True
    assert parsed["status"] == "scaled_to_zero"
    assert parsed["reason"] == "ScaledToZero"
    assert len(parsed["issues"]) == 0


def test_deployment_progress_deadline_exceeded():
    conditions = [
        {
            "type": "Progressing",
            "status": "False",
            "reason": "ProgressDeadlineExceeded",
            "message": "ReplicaSet timed out progressing",
        }
    ]
    dep = make_mock_deployment(name="failing-dep", desired=2, available=1, ready=1, conditions=conditions)
    parsed = parse_deployment(dep)

    assert parsed["isHealthy"] is False
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "ProgressFailure" in issue_types


def test_stale_observed_generation():
    dep = make_mock_deployment(name="stale-dep", generation=5, observed_generation=4)
    parsed = parse_deployment(dep)

    assert parsed["isHealthy"] is False
    issue_types = [i["type"] for i in parsed["issues"]]
    assert "StaleObservedGeneration" in issue_types
