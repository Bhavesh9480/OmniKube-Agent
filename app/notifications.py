"""Lightweight, zero-dependency webhook dispatcher for alert notifications.

Supports Discord, Slack, and generic JSON webhooks using Python's standard library.
"""

import json
import logging
import threading
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def build_slack_payload(event_type: str, alert: Dict[str, Any]) -> Dict[str, Any]:
    """Format alert for Slack incoming webhooks."""
    severity = alert.get("severity", "info").upper()
    title = alert.get("title", "Kubernetes Alert")
    kind = alert.get("resourceKind", "Resource")
    name = alert.get("resourceName", "unknown")
    ns = alert.get("namespace", "default")
    msg = alert.get("message", "")
    log_snippet = alert.get("lastLogSnippet")

    is_resolved = event_type == "alert_resolved" or alert.get("status") == "resolved"
    emoji = "✅" if is_resolved else ("🚨" if severity == "CRITICAL" else "⚠️")
    color = "#28a745" if is_resolved else ("#d73a49" if severity == "CRITICAL" else "#f66a0a")
    header_text = f"{emoji} *[{severity}]* {title} ({ns}/{name})"

    fields = [
        {"title": "Resource", "value": f"{kind}/{name}", "short": True},
        {"title": "Namespace", "value": ns, "short": True},
        {"title": "Severity", "value": severity, "short": True},
        {"title": "Status", "value": "RESOLVED" if is_resolved else "ACTIVE", "short": True},
    ]

    attachment: Dict[str, Any] = {
        "color": color,
        "title": title,
        "text": msg,
        "fields": fields,
        "ts": int(urllib.request.time.time()),
    }

    if log_snippet:
        attachment["fields"].append({
            "title": "Crash Log Snippet",
            "value": f"```{log_snippet[:500]}```",
            "short": False,
        })

    return {
        "text": header_text,
        "attachments": [attachment],
    }


def build_discord_payload(event_type: str, alert: Dict[str, Any]) -> Dict[str, Any]:
    """Format alert for Discord webhooks (embeds)."""
    severity = alert.get("severity", "info").upper()
    title = alert.get("title", "Kubernetes Alert")
    kind = alert.get("resourceKind", "Resource")
    name = alert.get("resourceName", "unknown")
    ns = alert.get("namespace", "default")
    msg = alert.get("message", "")
    log_snippet = alert.get("lastLogSnippet")

    is_resolved = event_type == "alert_resolved" or alert.get("status") == "resolved"
    color = 0x28A745 if is_resolved else (0xD73A49 if severity == "CRITICAL" else 0xF66A0A)
    status_str = "RESOLVED" if is_resolved else "ACTIVE"

    embed: Dict[str, Any] = {
        "title": f"[{severity}] {title}",
        "description": msg or "No description provided.",
        "color": color,
        "fields": [
            {"name": "Resource", "value": f"`{kind}/{name}`", "inline": True},
            {"name": "Namespace", "value": f"`{ns}`", "inline": True},
            {"name": "Status", "value": f"**{status_str}**", "inline": True},
        ],
    }

    if log_snippet:
        embed["fields"].append({
            "name": "Last Crash Log Snippet",
            "value": f"```{log_snippet[:400]}```",
            "inline": False,
        })

    return {
        "username": "OmniKube Alert",
        "embeds": [embed],
    }


def build_generic_payload(event_type: str, alert: Dict[str, Any]) -> Dict[str, Any]:
    """Format generic alert event payload."""
    return {
        "event": event_type,
        "alert": alert,
    }


def _send_post(url: str, payload: Dict[str, Any], timeout: int = 4) -> None:
    """Execute synchronous HTTP POST to webhook URL."""
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "OmniKube-Agent/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            logger.debug("Webhook dispatched successfully (HTTP %d)", resp.status)
    except Exception as e:
        logger.warning("Failed to dispatch alert webhook to %s: %s", url, e)


def dispatch_webhook(event_type: str, alert: Dict[str, Any], webhook_url: Optional[str] = None) -> None:
    """Dispatch webhook alert asynchronously in a daemon thread.

    Non-blocking, zero-dependency.
    """
    if not webhook_url:
        return

    url = webhook_url.strip()
    if not url:
        return

    # Choose appropriate payload builder based on webhook destination
    if "discord.com/api/webhooks" in url:
        payload = build_discord_payload(event_type, alert)
    elif "hooks.slack.com" in url:
        payload = build_slack_payload(event_type, alert)
    else:
        payload = build_generic_payload(event_type, alert)

    # Spawn daemon thread to prevent blocking caller
    t = threading.Thread(
        target=_send_post,
        args=(url, payload),
        name="WebhookDispatcher",
        daemon=True,
    )
    t.start()
