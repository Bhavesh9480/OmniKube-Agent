"""Kubernetes client initialization and lifecycle abstraction.

Supports three explicit modes:
- production: In-cluster ServiceAccount authentication (load_incluster_config)
- local-cluster: Developer local kubeconfig (load_kube_config)
- mock: Deliberate local simulation without cluster connection

Never falls back silently between modes.
"""

import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Attempt to import kubernetes components safely
try:
    from kubernetes import client, config, watch
    from kubernetes.client.exceptions import ApiException
    from kubernetes.config.config_exception import ConfigException
    K8S_PACKAGE_AVAILABLE = True
except ImportError:
    client = None  # type: ignore
    config = None  # type: ignore
    watch = None  # type: ignore
    ApiException = Exception  # type: ignore
    ConfigException = Exception  # type: ignore
    K8S_PACKAGE_AVAILABLE = False


class KubernetesClientManager:
    """Manages Kubernetes API clients and connection state."""

    def __init__(self, mode: str = "production"):
        self.mode = mode
        self.connected: bool = False
        self.is_mock: bool = False
        self.last_error: Optional[str] = None
        self.core_v1: Optional[Any] = None
        self.apps_v1: Optional[Any] = None
        self.discovery_v1: Optional[Any] = None

    def initialize(self) -> Tuple[bool, Optional[str]]:
        """Initialize the client according to the configured mode.
        
        Returns:
            Tuple of (success_flag, error_message)
        """
        if self.mode == "mock":
            logger.info("Initializing Kubernetes client in MOCK mode (simulated data).")
            self.connected = True
            self.is_mock = True
            self.last_error = None
            return True, None

        if not K8S_PACKAGE_AVAILABLE:
            err = "Official 'kubernetes' Python package is not installed."
            logger.error(err)
            self.connected = False
            self.last_error = err
            return False, err

        if self.mode == "local-cluster":
            logger.info("Initializing Kubernetes client using local kubeconfig (local-cluster mode).")
            try:
                config.load_kube_config()
                self._init_api_clients()
                self.connected = True
                self.is_mock = False
                self.last_error = None
                logger.info("Successfully connected to cluster via local kubeconfig.")
                return True, None
            except Exception as e:
                err = f"Failed to load local kubeconfig: {e}"
                logger.warning(err)
                self.connected = False
                self.last_error = err
                return False, err

        if self.mode == "production":
            logger.info("Initializing Kubernetes client in PRODUCTION mode (in-cluster authentication).")
            try:
                config.load_incluster_config()
                self._init_api_clients()
                self.connected = True
                self.is_mock = False
                self.last_error = None
                logger.info("Successfully loaded in-cluster Kubernetes configuration.")
                return True, None
            except Exception as e:
                err = f"Failed to load in-cluster configuration: {e}"
                logger.warning(err)
                self.connected = False
                self.last_error = err
                return False, err

        err = f"Invalid APP_MODE '{self.mode}'. Expected 'production', 'mock', or 'local-cluster'."
        logger.error(err)
        self.connected = False
        self.last_error = err
        return False, err

    def _init_api_clients(self) -> None:
        """Instantiate required API clients."""
        if not K8S_PACKAGE_AVAILABLE or client is None:
            return
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.discovery_v1 = client.DiscoveryV1Api()

    def get_status(self) -> Dict[str, Any]:
        """Return connectivity status dictionary."""
        return {
            "mode": self.mode,
            "connected": self.connected,
            "is_mock": self.is_mock,
            "package_available": K8S_PACKAGE_AVAILABLE,
            "error": self.last_error,
        }


# Global client manager instance (configured during startup)
k8s_manager = KubernetesClientManager()
