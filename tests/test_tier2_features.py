"""Unit tests for Tier 2 Kubernetes Monitoring Agent features.

Covers:
- Node collector (readiness, pressure conditions, allocatable resources)
- StatefulSet collector (replica shortages, conditions)
- PVC collector (Pending and Lost phase detection)
- Alert evaluation for Nodes, StatefulSets, and PVCs
- Resource cache additions (nodes, statefulsets, pvcs)
- Leader Election Manager (disabled default vs mock active)
- Scoped multi-namespace configuration parsing
"""

import pytest

from app.alerts import alert_manager
from app.cache import resource_cache
from app.collectors.nodes import parse_node
from app.collectors.pvcs import parse_pvc
from app.collectors.statefulsets import parse_statefulset
from app.config import Settings
from app.leader import LeaderElectionManager
from tests.fixtures.k8s_mocks import (
    make_mock_node,
    make_mock_pvc,
    make_mock_statefulset,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global cache and alerts before each test."""
    resource_cache.clear()
    alert_manager.clear()
    yield
    resource_cache.clear()
    alert_manager.clear()


# --- Node Collector Tests ---

def test_parse_node_healthy():
    raw_node = make_mock_node(name="worker-1", ready=True, kubelet_version="v1.30.1", roles=["worker"])
    parsed = parse_node(raw_node)

    assert parsed["name"] == "worker-1"
    assert parsed["ready"] is True
    assert parsed["isHealthy"] is True
    assert parsed["kubeletVersion"] == "v1.30.1"
    assert parsed["internalIP"] == "192.168.1.10"
    assert "worker" in parsed["roles"]
    assert parsed["allocatable"]["cpu"] == "8"
    assert len(parsed["pressures"]) == 0
    assert len(parsed["issues"]) == 0


def test_parse_node_not_ready():
    raw_node = make_mock_node(name="worker-down", ready=False)
    parsed = parse_node(raw_node)

    assert parsed["name"] == "worker-down"
    assert parsed["ready"] is False
    assert parsed["isHealthy"] is False
    assert len(parsed["issues"]) == 1
    assert parsed["issues"][0]["type"] == "NodeNotReady"


def test_parse_node_pressures():
    raw_node = make_mock_node(
        name="worker-stressed",
        ready=True,
        memory_pressure=True,
        disk_pressure=True,
        pid_pressure=False,
    )
    parsed = parse_node(raw_node)

    assert parsed["ready"] is True
    assert parsed["isHealthy"] is False
    assert "MemoryPressure" in parsed["pressures"]
    assert "DiskPressure" in parsed["pressures"]
    assert len(parsed["issues"]) == 2


# --- StatefulSet Collector Tests ---

def test_parse_statefulset_healthy():
    raw_sts = make_mock_statefulset(name="redis-cluster", desired=3, ready=3)
    parsed = parse_statefulset(raw_sts)

    assert parsed["name"] == "redis-cluster"
    assert parsed["desired"] == 3
    assert parsed["ready"] == 3
    assert parsed["isHealthy"] is True
    assert len(parsed["issues"]) == 0


def test_parse_statefulset_replica_shortage():
    raw_sts = make_mock_statefulset(name="kafka", desired=3, ready=1)
    parsed = parse_statefulset(raw_sts)

    assert parsed["name"] == "kafka"
    assert parsed["desired"] == 3
    assert parsed["ready"] == 1
    assert parsed["isHealthy"] is False
    assert len(parsed["issues"]) == 1
    assert parsed["issues"][0]["type"] == "StatefulSetReplicaShortage"
    assert "1/3 ready replicas" in parsed["issues"][0]["message"]


# --- PVC Collector Tests ---

def test_parse_pvc_bound_healthy():
    raw_pvc = make_mock_pvc(name="db-data", phase="Bound", storage="20Gi")
    parsed = parse_pvc(raw_pvc)

    assert parsed["name"] == "db-data"
    assert parsed["phase"] == "Bound"
    assert parsed["capacity"] == "20Gi"
    assert parsed["isHealthy"] is True
    assert len(parsed["issues"]) == 0


def test_parse_pvc_pending_issue():
    raw_pvc = make_mock_pvc(name="unbound-claim", phase="Pending")
    parsed = parse_pvc(raw_pvc)

    assert parsed["phase"] == "Pending"
    assert parsed["isHealthy"] is False
    assert len(parsed["issues"]) == 1
    assert parsed["issues"][0]["type"] == "PVCPending"


def test_parse_pvc_lost_issue():
    raw_pvc = make_mock_pvc(name="orphaned-claim", phase="Lost")
    parsed = parse_pvc(raw_pvc)

    assert parsed["phase"] == "Lost"
    assert parsed["isHealthy"] is False
    assert len(parsed["issues"]) == 1
    assert parsed["issues"][0]["type"] == "PVCLost"


# --- Tier 2 Alert Manager Evaluation Tests ---

def test_alert_manager_evaluates_tier2_resources():
    node_bad = parse_node(make_mock_node(name="node-fail", ready=False, memory_pressure=True))
    sts_short = parse_statefulset(make_mock_statefulset(name="sts-short", desired=5, ready=2))
    pvc_lost = parse_pvc(make_mock_pvc(name="pvc-lost", phase="Lost"))
    pvc_pending = parse_pvc(make_mock_pvc(name="pvc-pending", phase="Pending"))

    # Evaluate all resources including Tier 2
    alert_manager.evaluate(
        pods=[],
        deployments=[],
        services=[],
        nodes=[node_bad],
        statefulsets=[sts_short],
        pvcs=[pvc_lost, pvc_pending],
    )
    alerts = alert_manager.get_alerts()

    alert_titles = {a["title"] for a in alerts}
    assert "Node node-fail: NodeNotReady" in alert_titles
    assert "Node node-fail: NodeMemoryPressure" in alert_titles
    assert "StatefulSet sts-short replica shortage" in alert_titles
    assert "PVC pvc-lost: PVCLost" in alert_titles
    assert "PVC pvc-pending: PVCPending" in alert_titles

    # Verify severities
    for a in alerts:
        if a["resourceName"] == "node-fail" and "Not Ready" in a["title"]:
            assert a["severity"] == "critical"
        elif a["resourceName"] == "pvc-lost":
            assert a["severity"] == "critical"
        elif a["resourceName"] == "sts-short":
            assert a["severity"] == "warning"
        elif a["resourceName"] == "pvc-pending":
            assert a["severity"] == "warning"


# --- Resource Cache Tier 2 Storage Tests ---

def test_resource_cache_tier2_storage():
    node = parse_node(make_mock_node(name="node-c1"))
    sts = parse_statefulset(make_mock_statefulset(name="sts-c1"))
    pvc = parse_pvc(make_mock_pvc(name="pvc-c1"))

    resource_cache.set_nodes([node])
    resource_cache.set_statefulsets([sts])
    resource_cache.set_pvcs([pvc])

    assert len(resource_cache.get_nodes()) == 1
    assert resource_cache.get_nodes()[0]["name"] == "node-c1"
    assert len(resource_cache.get_statefulsets()) == 1
    assert resource_cache.get_statefulsets()[0]["name"] == "sts-c1"
    assert len(resource_cache.get_pvcs()) == 1
    assert resource_cache.get_pvcs()[0]["name"] == "pvc-c1"

    stats = resource_cache.get_stats()
    assert stats["nodes_count"] == 1
    assert stats["statefulsets_count"] == 1
    assert stats["pvcs_count"] == 1


# --- Leader Election Manager Tests ---

def test_leader_manager_disabled_by_default():
    mgr = LeaderElectionManager(enable_leader_election=False)
    # When disabled, agent acts as singleton and is immediately leader
    assert mgr.is_leader() is True
    mgr.start()
    assert mgr.is_leader() is True
    mgr.stop()
    assert mgr.is_leader() is True


def test_leader_manager_identity():
    mgr = LeaderElectionManager(
        enable_leader_election=True,
        holder_identity="pod-worker-test",
    )
    assert mgr.identity == "pod-worker-test"
    # Before starting election, is_leader is False
    assert mgr.is_leader() is False


# --- Scoped Multi-Namespace Configuration Tests ---

def test_settings_scoped_namespaces_parsing():
    s1 = Settings(watch_namespaces="production, staging, default")
    assert s1.get_monitored_namespaces() == ["production", "staging", "default"]

    s2 = Settings(watch_namespaces="")
    assert s2.get_monitored_namespaces() == []

    s3 = Settings(watch_namespaces="  single-namespace  ")
    assert s3.get_monitored_namespaces() == ["single-namespace"]
