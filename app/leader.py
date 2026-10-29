"""Native Kubernetes Lease-based Leader Election for High Availability (HA).

Zero-dependency implementation using Kubernetes coordination.k8s.io/v1 Leases.
Guarantees only one active agent replica performs watchers & alerts when scaled > 1.
"""

import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.config import settings
from app.kubernetes_client import k8s_manager

logger = logging.getLogger(__name__)

# Try import V1Lease and V1LeaseSpec safely
try:
    from kubernetes import client
    K8S_AVAILABLE = True
except ImportError:
    client = None
    K8S_AVAILABLE = False


class LeaderElectionManager:
    """Manages active leader status using native Kubernetes Leases."""

    def __init__(
        self,
        enable_leader_election: Optional[bool] = None,
        holder_identity: Optional[str] = None,
        lease_name: Optional[str] = None,
        namespace: Optional[str] = None,
        lease_duration: Optional[int] = None,
    ):
        enabled = (
            enable_leader_election
            if enable_leader_election is not None
            else getattr(settings, "enable_leader_election", False)
        )
        self.enabled: bool = enabled
        self._is_leader: bool = not enabled
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Unique pod identity (e.g. POD_NAME or hostname + PID)
        self.identity: str = (
            holder_identity
            or os.environ.get("POD_NAME", f"{socket.gethostname()}-{os.getpid()}")
        )
        self.leader_identity: Optional[str] = None
        self.lease_name: str = (
            lease_name
            or getattr(settings, "leader_election_lease_name", "omnikube-agent-leader")
        )
        self.namespace: str = (
            namespace
            or getattr(settings, "leader_election_namespace", "default")
        )
        self.lease_duration: int = (
            lease_duration
            or getattr(settings, "leader_election_lease_duration_seconds", 15)
        )

    def is_leader(self) -> bool:
        """Return True if this instance is the active leader or if leader election is disabled."""
        if not self.enabled:
            return True
        return self._is_leader

    def start(self) -> None:
        """Start leader election loop if enabled."""
        if not getattr(settings, "enable_leader_election", False):
            self._is_leader = True
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._leader_election_loop,
            name="LeaderElectionLoop",
            daemon=True,
        )
        self._thread.start()
        logger.info("Leader election started for identity '%s' (lease=%s/%s)", self.identity, self.namespace, self.lease_name)

    def stop(self) -> None:
        """Stop leader election loop."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._is_leader = not getattr(settings, "enable_leader_election", False)

    def _get_coordination_api(self) -> Optional[Any]:
        if not K8S_AVAILABLE or not k8s_manager.connected or k8s_manager.is_mock:
            return None
        return getattr(k8s_manager, "coordination_v1", None)

    def _leader_election_loop(self) -> None:
        """Periodically acquire or renew Kubernetes Lease."""
        renew_interval = max(3, int(self.lease_duration / 3))

        while not self._stop_event.is_set():
            coord_api = self._get_coordination_api()
            if not coord_api:
                # If mock or no coordination API, assume leader
                self._is_leader = True
                time.sleep(renew_interval)
                continue

            try:
                self._try_acquire_or_renew(coord_api)
            except Exception as e:
                logger.warning("Error in leader election lease renewal: %s", e)
                if self._is_leader:
                    logger.warning("Lost leadership due to lease renewal failure.")
                    self._is_leader = False

            time.sleep(renew_interval)

    def _try_acquire_or_renew(self, coord_api: Any) -> None:
        """Attempt to read, acquire, or renew the Lease object."""
        now = datetime.now(timezone.utc)

        # 1. Try reading existing lease
        try:
            lease = coord_api.read_namespaced_lease(
                name=self.lease_name,
                namespace=self.namespace,
                _request_timeout=3,
            )
        except Exception:
            lease = None

        if lease is None:
            # Lease does not exist -> Attempt to create
            new_lease = client.V1Lease(
                metadata=client.V1ObjectMeta(
                    name=self.lease_name,
                    namespace=self.namespace,
                ),
                spec=client.V1LeaseSpec(
                    holder_identity=self.identity,
                    lease_duration_seconds=self.lease_duration,
                    acquire_time=now,
                    renew_time=now,
                ),
            )
            try:
                coord_api.create_namespaced_lease(
                    namespace=self.namespace,
                    body=new_lease,
                    _request_timeout=3,
                )
                self._is_leader = True
                self.leader_identity = self.identity
                logger.info("Successfully acquired leadership lease '%s'.", self.lease_name)
                return
            except Exception as ex:
                logger.debug("Failed to create leadership lease: %s", ex)
                self._is_leader = False
                return

        # Lease exists -> Check if we own it or if it is expired
        spec = lease.spec or client.V1LeaseSpec()
        holder = spec.holder_identity
        renew_time = spec.renew_time

        is_current_holder = holder == self.identity
        is_expired = False

        if renew_time is not None:
            age = (now - renew_time).total_seconds()
            if age > self.lease_duration:
                is_expired = True
        else:
            is_expired = True

        if is_current_holder or is_expired:
            # Renew or takeover
            lease.spec.holder_identity = self.identity
            lease.spec.renew_time = now
            if not is_current_holder:
                lease.spec.acquire_time = now
                logger.info("Leadership expired from '%s'. Taking over as new leader.", holder)

            try:
                coord_api.replace_namespaced_lease(
                    name=self.lease_name,
                    namespace=self.namespace,
                    body=lease,
                    _request_timeout=3,
                )
                if not self._is_leader:
                    logger.info("Acquired leadership lease '%s'.", self.lease_name)
                self._is_leader = True
                self.leader_identity = self.identity
            except Exception as ex:
                logger.warning("Failed to renew leadership lease: %s", ex)
                self._is_leader = False
        else:
            self._is_leader = False
            self.leader_identity = holder


# Global leader manager instance
leader_manager = LeaderElectionManager()
