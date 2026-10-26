"""Alert evaluation engine with stable deduplication and auto-resolution."""

import hashlib
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)


def generate_alert_id(rule: str, kind: str, namespace: str, name: str) -> str:
    """Generate a stable, deterministic alert ID."""
    raw = f"{rule}:{kind}:{namespace}:{name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class AlertManager:
    """Manages active and resolved alerts with thread-safe deduplication."""

    def __init__(self, history_limit: int = 500):
        self.history_limit = history_limit
        # Key: alert_id -> alert dictionary
        self._active_alerts: Dict[str, Dict[str, Any]] = {}
        # Bounded queue of resolved alerts
        self._resolved_alerts: Deque[Dict[str, Any]] = deque(maxlen=history_limit)

    def _broadcast(self, fn) -> None:
        try:
            from app.broadcaster import broadcaster
            fn(broadcaster)
        except Exception:
            pass

    def evaluate(
        self,
        pods: List[Dict[str, Any]],
        deployments: List[Dict[str, Any]],
        services: List[Dict[str, Any]],
    ) -> None:
        """Run evaluation rules across current cached resources.
        
        Suppresses duplicate alerts, updates lastSeen, and auto-resolves
        alerts when their underlying problem clears.
        """
        now = datetime.now(timezone.utc).isoformat()
        detected_alerts: Dict[str, Dict[str, Any]] = {}

        # 1. Evaluate Pods
        for pod in pods:
            ns = pod.get("namespace", "default")
            name = pod.get("name", "unknown")
            issues = pod.get("issues", [])
            has_crash_loop = any(i.get("type") == "CrashLoopBackOff" for i in issues)

            for issue in issues:
                issue_type = issue.get("type", "")
                msg = issue.get("message", "")

                rule = None
                severity = "warning"
                title = ""

                if issue_type == "CrashLoopBackOff":
                    rule = "PodCrashLoop"
                    severity = "critical"
                    title = f"Pod {name} in CrashLoopBackOff"
                elif issue_type in ("ImagePullBackOff", "ErrImagePull"):
                    rule = "PodImagePullFailure"
                    severity = "critical"
                    title = f"Pod {name} image pull failure"
                elif issue_type in ("CreateContainerError", "CreateContainerConfigError"):
                    rule = "PodContainerCreateError"
                    severity = "critical"
                    title = f"Pod {name} container creation error"
                elif issue_type == "PodFailed":
                    rule = "PodFailed"
                    severity = "critical"
                    title = f"Pod {name} phase is Failed"
                elif issue_type == "ProlongedPending":
                    rule = "PodProlongedPending"
                    severity = "warning"
                    title = f"Pod {name} pending duration exceeded"
                elif issue_type == "HighRestartCount":
                    if not has_crash_loop:
                        rule = "PodRepeatedRestarts"
                        severity = "warning"
                        title = f"Pod {name} high container restart count"
                elif issue_type == "PodTerminating":
                    rule = "PodStuckTerminating"
                    severity = "warning"
                    title = f"Pod {name} terminating"

                if rule:
                    a_id = generate_alert_id(rule, "Pod", ns, name)
                    detected_alerts[a_id] = {
                        "id": a_id,
                        "severity": severity,
                        "title": title,
                        "resourceKind": "Pod",
                        "resourceName": name,
                        "namespace": ns,
                        "reason": issue_type,
                        "message": msg,
                    }

        # 2. Evaluate Deployments
        for dep in deployments:
            ns = dep.get("namespace", "default")
            name = dep.get("name", "unknown")

            for issue in dep.get("issues", []):
                issue_type = issue.get("type", "")
                msg = issue.get("message", "")

                rule = None
                severity = "warning"
                title = ""

                if issue_type == "ProgressFailure":
                    rule = "DeploymentProgressFailure"
                    severity = "critical"
                    title = f"Deployment {name} progress deadline exceeded"
                elif issue_type in ("UnavailableReplicas", "ReplicaShortage", "UnreadyReplicas"):
                    rule = "DeploymentReplicaShortage"
                    severity = "warning"
                    title = f"Deployment {name} replica shortage"
                elif issue_type == "StaleObservedGeneration":
                    rule = "DeploymentStaleGeneration"
                    severity = "info"
                    title = f"Deployment {name} stale observed generation"

                if rule:
                    a_id = generate_alert_id(rule, "Deployment", ns, name)
                    detected_alerts[a_id] = {
                        "id": a_id,
                        "severity": severity,
                        "title": title,
                        "resourceKind": "Deployment",
                        "resourceName": name,
                        "namespace": ns,
                        "reason": issue_type,
                        "message": msg,
                    }

        # 3. Evaluate Services
        for svc in services:
            ns = svc.get("namespace", "default")
            name = svc.get("name", "unknown")

            if svc.get("withoutReadyEndpoints"):
                for issue in svc.get("issues", []):
                    rule = "ServiceMissingEndpoints"
                    severity = "warning"
                    a_id = generate_alert_id(rule, "Service", ns, name)
                    detected_alerts[a_id] = {
                        "id": a_id,
                        "severity": severity,
                        "title": f"Service {name} lacks ready endpoints",
                        "resourceKind": "Service",
                        "resourceName": name,
                        "namespace": ns,
                        "reason": issue.get("type", "MissingReadyEndpoints"),
                        "message": issue.get("message", "Service has no ready endpoints"),
                    }

        # Update active alerts and deduplicate
        for a_id, a_data in detected_alerts.items():
            if a_id in self._active_alerts:
                # Update lastSeen and refresh message
                self._active_alerts[a_id]["lastSeen"] = now
                self._active_alerts[a_id]["message"] = a_data["message"]
            else:
                # New alert
                new_alert = {
                    **a_data,
                    "firstSeen": now,
                    "lastSeen": now,
                    "status": "active",
                }
                self._active_alerts[a_id] = new_alert
                self._broadcast(lambda b: b.broadcast_alert_event("alert_created", new_alert))

        # Auto-resolve alerts that are no longer detected
        resolved_ids = [aid for aid in self._active_alerts if aid not in detected_alerts]
        for aid in resolved_ids:
            alert = self._active_alerts.pop(aid)
            alert["status"] = "resolved"
            alert["lastSeen"] = now
            self._resolved_alerts.appendleft(alert)
            self._broadcast(lambda b: b.broadcast_alert_event("alert_resolved", alert))
            logger.info("Resolved alert %s (%s)", aid, alert["title"])

    def get_alerts(
        self,
        severity: Optional[str] = None,
        namespace: Optional[str] = None,
        limit: Optional[int] = None,
        include_resolved: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return list of active and optionally resolved alerts matching filters."""
        alerts = list(self._active_alerts.values())
        if include_resolved:
            alerts.extend(list(self._resolved_alerts))

        if severity:
            sev_lower = severity.lower()
            alerts = [a for a in alerts if a.get("severity", "").lower() == sev_lower]

        if namespace:
            alerts = [a for a in alerts if a.get("namespace") == namespace]

        if limit is not None and limit > 0:
            alerts = alerts[:limit]

        return alerts

    def get_counts(self) -> Dict[str, int]:
        """Return active alert counts by severity."""
        counts = {"critical": 0, "warning": 0, "info": 0, "total": 0}
        for alert in self._active_alerts.values():
            sev = alert.get("severity", "info").lower()
            if sev in counts:
                counts[sev] += 1
            else:
                counts["info"] += 1
            counts["total"] += 1
        return counts

    def clear(self) -> None:
        """Clear all active and resolved alerts."""
        self._active_alerts.clear()
        self._resolved_alerts.clear()


# Global alert manager instance
alert_manager = AlertManager()
