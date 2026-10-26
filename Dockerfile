# Multi-stage production Dockerfile for Kubernetes Monitoring Agent
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Final runtime image
FROM python:3.12-slim

# Set environment variables for production execution
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    APP_MODE=production \
    PORT=9999

WORKDIR /app

# Create unprivileged system user and group (UID 10001)
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /sbin/nologin -M appuser

# Copy installed Python packages from builder stage
COPY --from=builder /install /usr/local

# Copy application source code
COPY --chown=appuser:appgroup app/ /app/app/

# Switch to non-root user
USER 10001:10001

# Expose HTTP port
EXPOSE 9999

# Container liveness check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9999/healthz')" || exit 1

# Start the application with Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9999"]
