"""Configuration management for the Kubernetes Monitoring Agent."""

from typing import List, Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Runtime mode: "production" (in-cluster), "mock" (simulation), "local-cluster" (kubeconfig)
    app_mode: Literal["production", "mock", "local-cluster"] = Field(
        default="production",
        description="Application operational mode. Must be 'production', 'mock', or 'local-cluster'.",
    )

    # Agent metadata
    agent_version: str = Field(default="1.0.0", description="Version of the monitoring agent")

    # Network configuration
    host: str = Field(default="0.0.0.0", description="Host interface to bind")
    port: int = Field(default=9999, description="Port to listen on")
    log_level: str = Field(default="INFO", description="Logging level")

    # Thresholds for alert evaluation
    pending_pod_threshold_seconds: int = Field(
        default=300,
        description="Duration in seconds before a Pending pod is flagged as prolonged pending",
    )
    crash_loop_restart_threshold: int = Field(
        default=3,
        description="Restart count threshold above which a restarting container raises an alert",
    )
    cache_stale_threshold_seconds: int = Field(
        default=120,
        description="Seconds without sync before cached data is flagged as stale",
    )
    alert_history_limit: int = Field(
        default=500,
        description="Maximum number of resolved alerts to retain in memory",
    )
    reconciliation_interval_seconds: int = Field(
        default=60,
        description="Interval in seconds for full cluster reconciliation loops",
    )

    # Tier 1 Lightweight Enhancements
    enable_metrics_server: bool = Field(
        default=True,
        description="Query native metrics.k8s.io for live CPU/RAM if available",
    )
    enable_crash_logs: bool = Field(
        default=True,
        description="Auto-fetch tail logs on container crash/failure",
    )
    crash_log_tail_lines: int = Field(
        default=20,
        description="Lines of logs to fetch from crashed containers",
    )
    alert_webhook_url: Optional[str] = Field(
        default=None,
        description="Incoming webhook URL for Slack, Discord, or generic HTTP POST alerts",
    )
    alert_flapping_cooldown_seconds: int = Field(
        default=0,
        description="Cooldown seconds before auto-resolving an alert to prevent flapping (0 = immediate)",
    )

    # Tier 2 Production & Multi-Tenancy Settings
    watch_namespaces: Optional[str] = Field(
        default=None,
        description="Comma-separated namespaces to monitor (e.g. 'prod,stage'). If empty, monitors cluster-wide.",
    )
    enable_leader_election: bool = Field(
        default=False,
        description="Enable Kubernetes Lease-based leader election for HA multi-replica deployments",
    )
    leader_election_lease_name: str = Field(
        default="omnikube-agent-leader",
        description="Name of the coordination.k8s.io Lease object",
    )
    leader_election_namespace: str = Field(
        default="default",
        description="Namespace hosting the leader election lease",
    )
    leader_election_lease_duration_seconds: int = Field(
        default=15,
        description="Lease validity duration in seconds",
    )

    def get_monitored_namespaces(self) -> List[str]:
        """Return list of explicit namespaces to watch, or empty list if cluster-wide."""
        if not self.watch_namespaces:
            return []
        return [ns.strip() for ns in self.watch_namespaces.split(",") if ns.strip()]

    # Security settings
    cors_allowed_origins: List[str] = Field(
        default_factory=list,
        description="Allowed CORS origins. Default is empty (CORS disabled/restricted).",
    )


# Singleton settings instance
settings = Settings()
