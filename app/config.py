"""Configuration management for the Kubernetes Monitoring Agent."""

from typing import List, Literal
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

    # Security settings
    cors_allowed_origins: List[str] = Field(
        default_factory=list,
        description="Allowed CORS origins. Default is empty (CORS disabled/restricted).",
    )


# Singleton settings instance
settings = Settings()
