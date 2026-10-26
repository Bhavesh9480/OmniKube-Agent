"""Main FastAPI application entrypoint for the Kubernetes Monitoring Agent."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.routes import router as api_router
from app.config import settings
from app.kubernetes_client import k8s_manager
from app.watchers import cluster_watcher

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("k8s_agent")


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add secure HTTP headers to all API responses."""

    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store, max-age=0"
        return response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle."""
    import asyncio
    from app.broadcaster import broadcaster

    logger.info("Starting Kubernetes Monitoring Agent v%s (mode=%s)...", settings.agent_version, settings.app_mode)

    # Initialize Kubernetes client
    k8s_manager.mode = settings.app_mode
    success, error = k8s_manager.initialize()
    if not success:
        logger.warning("Kubernetes initialization status: %s", error)

    # Start WebSocket broadcaster heartbeat
    loop = asyncio.get_running_loop()
    broadcaster.start_heartbeat(loop)

    # Start watchers or mock simulation
    cluster_watcher.start()

    yield

    # Graceful shutdown
    logger.info("Shutting down Kubernetes Monitoring Agent...")
    broadcaster.stop_heartbeat()
    cluster_watcher.stop()


app = FastAPI(
    title="Kubernetes Monitoring Agent",
    description="Lightweight cluster-wide resource monitoring agent for Kubernetes.",
    version=settings.agent_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Security headers
app.add_middleware(SecurityHeadersMiddleware)

# CORS configuration (allows configured origins and default local frontend port 9998)
allowed_origins = list(settings.cors_allowed_origins)
for default_dev in ("http://localhost:9998", "http://127.0.0.1:9998", "http://localhost:5173", "http://127.0.0.1:5173"):
    if default_dev not in allowed_origins:
        allowed_origins.append(default_dev)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Include routes
app.include_router(api_router)


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "Kubernetes Monitoring Agent",
        "version": settings.agent_version,
        "mode": settings.app_mode,
        "docs": "/docs",
        "health": "/api/health",
        "summary": "/api/summary",
    }
