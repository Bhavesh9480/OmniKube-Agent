"""Pydantic schemas validating all API request and response contracts."""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


# --- Cluster & Summary Schemas ---

class ClusterStatus(BaseModel):
    connected: bool = Field(description="Whether the Kubernetes API is currently connected and reachable")
    lastSuccessfulSync: Optional[str] = Field(default=None, description="ISO timestamp of last successful cluster sync")


class PodsSummary(BaseModel):
    total: int = Field(default=0, description="Total number of monitored pods")
    running: int = Field(default=0, description="Pods in Running phase")
    pending: int = Field(default=0, description="Pods in Pending phase")
    failed: int = Field(default=0, description="Pods in Failed phase")
    unknown: int = Field(default=0, description="Pods in Unknown phase")
    terminating: int = Field(default=0, description="Pods currently terminating")
    unhealthy: int = Field(default=0, description="Total unhealthy pods")


class DeploymentsSummary(BaseModel):
    total: int = Field(default=0, description="Total number of monitored deployments")
    healthy: int = Field(default=0, description="Deployments meeting full replica availability")
    unhealthy: int = Field(default=0, description="Deployments experiencing shortages or progress failures")


class ServicesSummary(BaseModel):
    total: int = Field(default=0, description="Total number of monitored services")
    withoutReadyEndpoints: int = Field(default=0, description="Services with selectors lacking ready endpoints")


class AlertsSummary(BaseModel):
    critical: int = Field(default=0, description="Active critical alerts")
    warning: int = Field(default=0, description="Active warning alerts")
    info: int = Field(default=0, description="Active info alerts")
    total: int = Field(default=0, description="Total active alerts")


class SummaryResponse(BaseModel):
    status: Literal["healthy", "warning", "critical", "unknown"] = Field(
        description="Overall cluster monitoring health status"
    )
    cluster: ClusterStatus
    pods: PodsSummary
    deployments: DeploymentsSummary
    services: ServicesSummary
    alerts: AlertsSummary


# --- Pods Schemas ---

class ContainerItem(BaseModel):
    name: str
    image: str
    ready: bool
    restartCount: int
    state: str
    reason: Optional[str] = None
    message: Optional[str] = None
    exitCode: Optional[int] = None


class PodItem(BaseModel):
    namespace: str
    name: str
    uid: str
    phase: str
    node: Optional[str] = None
    podIP: Optional[str] = None
    ready: bool
    readyContainers: int
    totalContainers: int
    restarts: int
    reason: str
    containers: List[ContainerItem] = Field(default_factory=list)
    createdAt: Optional[str] = None
    deletionTimestamp: Optional[str] = None
    isTerminating: Optional[bool] = False
    isUnhealthy: Optional[bool] = False


class PodsListResponse(BaseModel):
    items: List[PodItem]
    count: int
    continueToken: Optional[str] = None


# --- Deployments Schemas ---

class DeploymentItem(BaseModel):
    namespace: str
    name: str
    desired: int
    updated: int
    ready: int
    available: int
    unavailable: int
    status: str
    reason: str
    generation: Optional[int] = None
    observedGeneration: Optional[int] = None
    createdAt: Optional[str] = None


class DeploymentsListResponse(BaseModel):
    items: List[DeploymentItem]
    count: int
    lastUpdated: Optional[str] = None


# --- Alerts Schemas ---

class AlertItem(BaseModel):
    id: str = Field(description="Stable, deterministic alert ID")
    severity: Literal["critical", "warning", "info"]
    title: str
    resourceKind: str
    resourceName: str
    namespace: str
    reason: str
    message: str
    firstSeen: str
    lastSeen: str
    status: Literal["active", "resolved"]


class AlertsListResponse(BaseModel):
    items: List[AlertItem]
    count: int


# --- Health Schemas ---

class LivenessResponse(BaseModel):
    status: Literal["ok"] = "ok"


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    reason: Optional[str] = None


class DetailedHealthResponse(BaseModel):
    status: Literal["healthy", "degraded", "unhealthy"]
    agentVersion: str
    mode: str
    isMock: bool
    kubernetes: Dict[str, Any]
    cache: Dict[str, Any]
    watchers: Dict[str, str]
    syncErrors: Dict[str, str]


# --- Services & EndpointSlices Schemas ---

class ServicePortItem(BaseModel):
    name: Optional[str] = None
    port: Optional[int] = None
    targetPort: Optional[Any] = None
    protocol: Optional[str] = "TCP"
    nodePort: Optional[int] = None


class ServiceItem(BaseModel):
    namespace: str
    name: str
    type: str
    clusterIP: Optional[str] = None
    ports: List[Dict[str, Any]] = Field(default_factory=list)
    selector: Dict[str, str] = Field(default_factory=dict)
    createdAt: Optional[str] = None
    expectsEndpoints: bool = True
    hasSelector: bool = True
    readyEndpoints: int = 0
    totalEndpoints: int = 0
    withoutReadyEndpoints: bool = False
    isHealthy: bool = True
    issues: List[Dict[str, Any]] = Field(default_factory=list)


class ServicesListResponse(BaseModel):
    items: List[ServiceItem]
    count: int
    lastUpdated: Optional[str] = None


class EndpointSliceItem(BaseModel):
    namespace: str
    name: str
    serviceName: Optional[str] = None
    addressType: str = "IPv4"
    endpoints: List[Dict[str, Any]] = Field(default_factory=list)
    ports: List[Dict[str, Any]] = Field(default_factory=list)
    readyCount: int = 0
    totalEndpoints: int = 0


class EndpointSlicesListResponse(BaseModel):
    items: List[EndpointSliceItem]
    count: int
    lastUpdated: Optional[str] = None


# --- Events Schemas ---

class EventItem(BaseModel):
    namespace: str
    name: str
    reason: str
    message: str
    type: str
    count: int = 1
    firstTimestamp: Optional[str] = None
    lastTimestamp: Optional[str] = None
    targetKind: str
    targetName: str
    targetNamespace: str


class EventsListResponse(BaseModel):
    items: List[EventItem]
    count: int


# --- Metrics Status Schema ---

class MetricsStatusResponse(BaseModel):
    configured: bool = False
    source: Optional[str] = None
    message: str

