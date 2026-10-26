"""API routes implementation for the Kubernetes Monitoring Agent."""

from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Query, Response, WebSocket, WebSocketDisconnect, status

from app.alerts import alert_manager
from app.api.schemas import (
    AlertItem,
    AlertsListResponse,
    AlertsSummary,
    ClusterStatus,
    DeploymentItem,
    DeploymentsListResponse,
    DeploymentsSummary,
    DetailedHealthResponse,
    EndpointSliceItem,
    EndpointSlicesListResponse,
    EventItem,
    EventsListResponse,
    LivenessResponse,
    MetricsStatusResponse,
    PodItem,
    PodsListResponse,
    PodsSummary,
    ReadinessResponse,
    ServiceItem,
    ServicesListResponse,
    ServicesSummary,
    SummaryResponse,
)
from app.broadcaster import broadcaster
from app.cache import resource_cache
from app.config import settings
from app.health import check_liveness, check_readiness, get_detailed_health
from app.kubernetes_client import k8s_manager
from app.watchers import cluster_watcher

router = APIRouter()


@router.get(
    "/api/summary",
    response_model=SummaryResponse,
    summary="Cluster health overview and resource counters",
    tags=["Monitoring"],
)
def get_summary() -> SummaryResponse:
    """Return cluster-wide health status, connectivity, and aggregated metrics."""
    pods = resource_cache.get_pods()
    deployments = resource_cache.get_deployments()
    services = resource_cache.get_services()
    alert_counts = alert_manager.get_counts()

    is_connected = k8s_manager.connected or (settings.app_mode == "mock")
    last_sync = resource_cache.get_last_successful_sync()

    # Aggregate pod counts
    pod_summary = PodsSummary(
        total=len(pods),
        running=sum(1 for p in pods if p.get("phase") == "Running"),
        pending=sum(1 for p in pods if p.get("phase") == "Pending"),
        failed=sum(1 for p in pods if p.get("phase") == "Failed"),
        unknown=sum(1 for p in pods if p.get("phase") == "Unknown"),
        terminating=sum(1 for p in pods if p.get("isTerminating")),
        unhealthy=sum(1 for p in pods if p.get("isUnhealthy")),
    )

    # Aggregate deployment counts
    dep_summary = DeploymentsSummary(
        total=len(deployments),
        healthy=sum(1 for d in deployments if d.get("isHealthy")),
        unhealthy=sum(1 for d in deployments if not d.get("isHealthy")),
    )

    # Aggregate service counts
    svc_summary = ServicesSummary(
        total=len(services),
        withoutReadyEndpoints=sum(1 for s in services if s.get("withoutReadyEndpoints")),
    )

    # Compute overall status
    if not is_connected:
        overall_status = "critical" if settings.app_mode == "production" else "unknown"
    elif alert_counts["critical"] > 0 or pod_summary.failed > 0:
        overall_status = "critical"
    elif (
        alert_counts["warning"] > 0
        or pod_summary.unhealthy > 0
        or dep_summary.unhealthy > 0
        or svc_summary.withoutReadyEndpoints > 0
    ):
        overall_status = "warning"
    else:
        overall_status = "healthy"

    return SummaryResponse(
        status=overall_status,
        cluster=ClusterStatus(
            connected=is_connected,
            lastSuccessfulSync=last_sync.isoformat() if last_sync else None,
        ),
        pods=pod_summary,
        deployments=dep_summary,
        services=svc_summary,
        alerts=AlertsSummary(**alert_counts),
    )


@router.get(
    "/api/pods",
    response_model=PodsListResponse,
    summary="List pods with health status and container details",
    tags=["Monitoring"],
)
def get_pods(
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    phase: Optional[str] = Query(None, description="Filter by pod phase (Running, Pending, Failed, etc.)"),
    attention: Optional[bool] = Query(None, description="Filter for pods requiring attention (unhealthy or terminating)"),
    limit: Optional[int] = Query(None, ge=1, description="Maximum number of items to return"),
    continue_token: Optional[str] = Query(None, description="Pagination cursor token"),
) -> PodsListResponse:
    """Return monitored pod details across nodes and namespaces."""
    pods = resource_cache.get_pods()

    # Apply filters
    if namespace:
        pods = [p for p in pods if p.get("namespace") == namespace]

    if phase:
        pods = [p for p in pods if p.get("phase", "").lower() == phase.lower()]

    if attention is not None:
        if attention:
            pods = [p for p in pods if p.get("isUnhealthy") or p.get("isTerminating")]
        else:
            pods = [p for p in pods if not p.get("isUnhealthy") and not p.get("isTerminating")]

    # Optional pagination
    offset = 0
    if continue_token:
        try:
            offset = int(continue_token)
        except ValueError:
            offset = 0

    total_count = len(pods)
    if offset < total_count:
        pods = pods[offset:]
    else:
        pods = []

    next_token = None
    if limit is not None and len(pods) > limit:
        next_token = str(offset + limit)
        pods = pods[:limit]

    items = [PodItem(**p) for p in pods]
    return PodsListResponse(items=items, count=len(items), continueToken=next_token)


@router.get(
    "/api/deployments",
    response_model=DeploymentsListResponse,
    summary="List deployments with replica availability and condition tracking",
    tags=["Monitoring"],
)
def get_deployments(
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    attention: Optional[bool] = Query(None, description="Filter for deployments with shortages or failures"),
) -> DeploymentsListResponse:
    """Return monitored deployments across namespaces."""
    deployments = resource_cache.get_deployments()

    if namespace:
        deployments = [d for d in deployments if d.get("namespace") == namespace]

    if attention is not None:
        if attention:
            deployments = [d for d in deployments if not d.get("isHealthy")]
        else:
            deployments = [d for d in deployments if d.get("isHealthy")]

    last_sync = resource_cache.get_last_successful_sync()

    items = [DeploymentItem(**d) for d in deployments]
    return DeploymentsListResponse(
        items=items,
        count=len(items),
        lastUpdated=last_sync.isoformat() if last_sync else None,
    )


@router.get(
    "/api/alerts",
    response_model=AlertsListResponse,
    summary="List active and resolved alerts",
    tags=["Monitoring"],
)
def get_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity: critical, warning, info"),
    namespace: Optional[str] = Query(None, description="Filter by resource namespace"),
    limit: Optional[int] = Query(None, ge=1, description="Maximum number of alerts to return"),
) -> AlertsListResponse:
    """Return current and historical alerts with stable IDs and resolution status."""
    alerts = alert_manager.get_alerts(severity=severity, namespace=namespace, limit=limit)
    items = [AlertItem(**a) for a in alerts]
    return AlertsListResponse(items=items, count=len(items))


@router.get(
    "/healthz",
    response_model=LivenessResponse,
    summary="Process liveness probe",
    tags=["Probes"],
)
def get_healthz() -> LivenessResponse:
    """Liveness probe confirming the application process is running."""
    res = check_liveness()
    return LivenessResponse(**res)


@router.get(
    "/readyz",
    response_model=ReadinessResponse,
    summary="Service readiness probe",
    tags=["Probes"],
)
def get_readyz(response: Response) -> ReadinessResponse:
    """Readiness probe verifying API connectivity and cache freshness."""
    is_ready, details = check_readiness()
    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(**details)


@router.get(
    "/api/health",
    response_model=DetailedHealthResponse,
    summary="Detailed diagnostic health status",
    tags=["Probes"],
)
def get_health() -> DetailedHealthResponse:
    """Return diagnostic telemetry for API connectivity, cache freshness, and watchers."""
    watcher_status = cluster_watcher.get_status()
    diagnostics = get_detailed_health(watcher_status)
    return DetailedHealthResponse(**diagnostics)


@router.get(
    "/api/services",
    response_model=ServicesListResponse,
    summary="List services with endpoint readiness and selector status",
    tags=["Monitoring"],
)
def get_services(
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    attention: Optional[bool] = Query(None, description="Filter for services without ready endpoints"),
) -> ServicesListResponse:
    """Return monitored services across namespaces."""
    services = resource_cache.get_services()

    if namespace:
        services = [s for s in services if s.get("namespace") == namespace]

    if attention is not None:
        if attention:
            services = [s for s in services if s.get("withoutReadyEndpoints")]
        else:
            services = [s for s in services if not s.get("withoutReadyEndpoints")]

    last_sync = resource_cache.get_last_successful_sync()
    items = [ServiceItem(**s) for s in services]
    return ServicesListResponse(
        items=items,
        count=len(items),
        lastUpdated=last_sync.isoformat() if last_sync else None,
    )


@router.get(
    "/api/endpointslices",
    response_model=EndpointSlicesListResponse,
    summary="List EndpointSlices with address and ready status tracking",
    tags=["Monitoring"],
)
def get_endpointslices(
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    service_name: Optional[str] = Query(None, description="Filter by associated service name"),
) -> EndpointSlicesListResponse:
    """Return monitored EndpointSlices across namespaces."""
    slices = resource_cache.get_endpointslices()

    if namespace:
        slices = [s for s in slices if s.get("namespace") == namespace]

    if service_name:
        slices = [s for s in slices if s.get("serviceName") == service_name]

    last_sync = resource_cache.get_last_successful_sync()
    items = [EndpointSliceItem(**s) for s in slices]
    return EndpointSlicesListResponse(
        items=items,
        count=len(items),
        lastUpdated=last_sync.isoformat() if last_sync else None,
    )


@router.get(
    "/api/events",
    response_model=EventsListResponse,
    summary="List recent Kubernetes cluster events",
    tags=["Monitoring"],
)
def get_events(
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    type: Optional[str] = Query(None, description="Filter by event type (Normal, Warning)"),
    limit: Optional[int] = Query(100, ge=1, le=500, description="Maximum number of events to return"),
) -> EventsListResponse:
    """Return recent cluster events ordered by timestamp."""
    events = resource_cache.get_events()

    if namespace:
        events = [e for e in events if e.get("namespace") == namespace]

    if type:
        events = [e for e in events if e.get("type", "").lower() == type.lower()]

    if limit:
        events = events[:limit]

    items = [EventItem(**e) for e in events]
    return EventsListResponse(items=items, count=len(items))


@router.get(
    "/api/metrics/status",
    response_model=MetricsStatusResponse,
    summary="Check Prometheus and metrics-server configuration status",
    tags=["Monitoring"],
)
def get_metrics_status() -> MetricsStatusResponse:
    """Explicitly verify whether real cluster metrics collection is configured."""
    return MetricsStatusResponse(
        configured=False,
        source=None,
        message="Metrics source not configured. Kubernetes metrics-server or Prometheus integration is required to collect CPU, memory, and network metrics.",
    )


@router.websocket("/api/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time bidirectional WebSocket stream for cluster updates and heartbeats."""
    await broadcaster.connect(websocket)
    try:
        while True:
            # Keep connection open and receive optional client ping/control messages
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        broadcaster.disconnect(websocket)
    except Exception:
        broadcaster.disconnect(websocket)

