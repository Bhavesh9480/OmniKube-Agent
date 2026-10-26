"""Unit tests for Kubernetes Event collector."""

from app.collectors.events import parse_event


def test_parse_warning_event():
    raw_event = {
        "metadata": {"namespace": "production", "name": "ev-fail-1"},
        "involvedObject": {
            "kind": "Pod",
            "name": "payment-api-xyz",
            "namespace": "production",
        },
        "reason": "FailedScheduling",
        "message": "0/3 nodes are available: 3 Insufficient memory.",
        "type": "Warning",
        "count": 5,
        "firstTimestamp": "2026-10-26T10:00:00Z",
        "lastTimestamp": "2026-10-26T10:15:00Z",
    }

    parsed = parse_event(raw_event)
    assert parsed["namespace"] == "production"
    assert parsed["name"] == "ev-fail-1"
    assert parsed["reason"] == "FailedScheduling"
    assert parsed["type"] == "Warning"
    assert parsed["count"] == 5
    assert parsed["targetKind"] == "Pod"
    assert parsed["targetName"] == "payment-api-xyz"
    assert "Insufficient memory" in parsed["message"]
