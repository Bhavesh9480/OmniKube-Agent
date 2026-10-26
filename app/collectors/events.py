"""Collector and parser for Kubernetes Event resources."""

from typing import Any, Dict
from app.collectors.pods import _get, _format_datetime


def parse_event(event: Any) -> Dict[str, Any]:
    """Parse a Kubernetes CoreV1 Event object into a diagnostic dictionary."""
    metadata = _get(event, "metadata")
    involved_object = _get(event, "involved_object", _get(event, "involvedObject"))

    namespace = _get(metadata, "namespace", "default")
    name = _get(metadata, "name", "unknown")

    reason = _get(event, "reason", "Unknown")
    message = _get(event, "message", "")
    event_type = _get(event, "type", "Normal")
    count = int(_get(event, "count", 1) or 1)

    first_timestamp = _get(event, "first_timestamp", _get(event, "firstTimestamp"))
    last_timestamp = _get(event, "last_timestamp", _get(event, "lastTimestamp"))
    event_time = _get(event, "event_time", _get(event, "eventTime"))

    # Target resource info
    target_kind = _get(involved_object, "kind", "Unknown") if involved_object else "Unknown"
    target_name = _get(involved_object, "name", "Unknown") if involved_object else "Unknown"
    target_namespace = _get(involved_object, "namespace", namespace) if involved_object else namespace

    return {
        "namespace": namespace,
        "name": name,
        "reason": reason,
        "message": message,
        "type": event_type,
        "count": count,
        "firstTimestamp": _format_datetime(first_timestamp),
        "lastTimestamp": _format_datetime(last_timestamp or event_time),
        "targetKind": target_kind,
        "targetName": target_name,
        "targetNamespace": target_namespace,
    }
