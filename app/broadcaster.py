"""Real-time WebSocket broadcaster for Kubernetes resource updates and heartbeats."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class Broadcaster:
    """Thread-safe WebSocket manager broadcasting cluster changes to connected UI clients."""

    def __init__(self):
        self._active_connections: Set[WebSocket] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def connect(self, websocket: WebSocket) -> None:
        """Register a new active WebSocket connection and send initial snapshot."""
        await websocket.accept()
        self._active_connections.add(websocket)
        logger.info("WebSocket client connected. Total clients: %d", len(self._active_connections))

        # Send initial snapshot immediately upon connection
        try:
            snapshot = self._build_snapshot()
            await websocket.send_text(json.dumps(snapshot, default=str))
        except Exception as e:
            logger.error("Failed to send initial snapshot to WebSocket client: %s", e)

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a closed or failed WebSocket connection."""
        self._active_connections.discard(websocket)
        logger.info("WebSocket client disconnected. Total clients: %d", len(self._active_connections))

    def _build_snapshot(self) -> Dict[str, Any]:
        """Generate full current state snapshot from cache and alert manager."""
        from app.cache import resource_cache
        from app.alerts import alert_manager
        from app.api.routes import get_summary

        now = datetime.now(timezone.utc).isoformat()
        last_sync = resource_cache.get_last_successful_sync()

        try:
            summary = get_summary().model_dump()
        except Exception:
            summary = None

        return {
            "type": "snapshot",
            "timestamp": now,
            "data": {
                "summary": summary,
                "pods": resource_cache.get_pods(),
                "deployments": resource_cache.get_deployments(),
                "services": resource_cache.get_services(),
                "endpointslices": resource_cache.get_endpointslices(),
                "alerts": alert_manager.get_alerts(),
                "events": resource_cache.get_events()[:100],
                "cacheFresh": resource_cache.is_fresh(),
                "initialSyncCompleted": resource_cache.is_initial_sync_completed(),
                "lastSuccessfulSync": last_sync.isoformat() if last_sync else None,
            },
        }

    async def _broadcast_async(self, message: Dict[str, Any]) -> None:
        """Send message to all active clients, pruning failed connections."""
        if not self._active_connections:
            return

        payload = json.dumps(message, default=str)
        dead_connections: List[WebSocket] = []

        for ws in list(self._active_connections):
            try:
                await ws.send_text(payload)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            self._active_connections.discard(ws)

    def broadcast(self, message: Dict[str, Any]) -> None:
        """Thread-safe entrypoint for background threads to broadcast messages."""
        if not self._active_connections:
            return

        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._broadcast_async(message), self._loop)
        else:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._broadcast_async(message))
            except RuntimeError:
                pass

    def broadcast_resource_updated(self, kind: str, resource: Dict[str, Any]) -> None:
        """Broadcast resource create/update event."""
        self.broadcast({
            "type": "resource_updated",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resourceKind": kind,
            "data": resource,
        })

    def broadcast_resource_deleted(self, kind: str, namespace: str, name: str) -> None:
        """Broadcast resource deletion event."""
        self.broadcast({
            "type": "resource_deleted",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resourceKind": kind,
            "namespace": namespace,
            "name": name,
        })

    def broadcast_alert_event(self, alert_type: str, alert: Dict[str, Any]) -> None:
        """Broadcast alert creation, update, or resolution."""
        self.broadcast({
            "type": alert_type,  # alert_created, alert_updated, alert_resolved
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": alert,
        })

    def broadcast_sync_status(self, status: str, last_sync: Optional[str] = None) -> None:
        """Broadcast cluster cache sync status."""
        self.broadcast({
            "type": "sync_status",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "lastSuccessfulSync": last_sync,
        })

    async def start_heartbeat_loop(self) -> None:
        """Send periodic heartbeat every 15 seconds."""
        from app.cache import resource_cache

        while True:
            try:
                await asyncio.sleep(15)
                if self._active_connections:
                    last_sync = resource_cache.get_last_successful_sync()
                    msg = {
                        "type": "heartbeat",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "cacheFresh": resource_cache.is_fresh(),
                        "ageSeconds": resource_cache.get_cache_age_seconds(),
                        "lastSuccessfulSync": last_sync.isoformat() if last_sync else None,
                    }
                    await self._broadcast_async(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in broadcaster heartbeat loop: %s", e)

    def start_heartbeat(self, loop: asyncio.AbstractEventLoop) -> None:
        self.set_loop(loop)
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = loop.create_task(self.start_heartbeat_loop())

    def stop_heartbeat(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()


# Singleton instance
broadcaster = Broadcaster()
