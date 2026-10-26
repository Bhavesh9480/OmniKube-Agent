"""Unit tests for Kubernetes Service and EndpointSlice association."""

from app.collectors.endpointslices import parse_endpointslice
from app.collectors.services import parse_service
from tests.fixtures.k8s_mocks import make_mock_endpointslice, make_mock_service


def test_service_with_healthy_endpoints():
    ep_slice = make_mock_endpointslice(
        name="api-svc-slice-1",
        service_name="api-svc",
        endpoints=[
            {"addresses": ["10.244.0.10"], "conditions": {"ready": True}},
            {"addresses": ["10.244.0.11"], "conditions": {"ready": True}},
        ],
    )
    parsed_slice = parse_endpointslice(ep_slice)

    svc = make_mock_service(name="api-svc", selector={"app": "api"})
    parsed_svc = parse_service(svc, [parsed_slice])

    assert parsed_svc["name"] == "api-svc"
    assert parsed_svc["isHealthy"] is True
    assert parsed_svc["withoutReadyEndpoints"] is False
    assert parsed_svc["readyEndpoints"] == 2
    assert parsed_svc["totalEndpoints"] == 2
    assert len(parsed_svc["issues"]) == 0


def test_service_with_selector_lacking_ready_endpoints():
    """Service expects endpoints (selector set), but all endpoints are unready."""
    ep_slice = make_mock_endpointslice(
        name="web-svc-slice-1",
        service_name="web-svc",
        endpoints=[
            {"addresses": ["10.244.0.12"], "conditions": {"ready": False}},
        ],
    )
    parsed_slice = parse_endpointslice(ep_slice)

    svc = make_mock_service(name="web-svc", selector={"app": "web"})
    parsed_svc = parse_service(svc, [parsed_slice])

    assert parsed_svc["withoutReadyEndpoints"] is True
    assert parsed_svc["isHealthy"] is False
    assert parsed_svc["readyEndpoints"] == 0
    issue_types = [i["type"] for i in parsed_svc["issues"]]
    assert "MissingReadyEndpoints" in issue_types


def test_service_without_selector_not_classified_broken():
    """Services without selectors (e.g. ExternalName, manual headless) are not classified as broken."""
    svc_external = make_mock_service(
        name="ext-db",
        service_type="ExternalName",
        cluster_ip=None,
        selector={},
    )
    parsed_svc = parse_service(svc_external, [])

    assert parsed_svc["name"] == "ext-db"
    assert parsed_svc["withoutReadyEndpoints"] is False
    assert parsed_svc["isHealthy"] is True
    assert parsed_svc["expectsEndpoints"] is False
    assert len(parsed_svc["issues"]) == 0
