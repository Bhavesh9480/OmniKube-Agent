# OmniKube Agent

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes%20Client-v29+-326ce5.svg?logo=kubernetes&logoColor=white)](https://github.com/kubernetes-client/python)
[![Tests](https://img.shields.io/badge/Tests-37%20Passed-brightgreen.svg)](#test-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A lightweight, enterprise-grade Kubernetes cluster monitoring agent built with **Python 3.12**, **FastAPI**, and the official **Kubernetes Python client**. 

**OmniKube Agent** is engineered to run as a dedicated Pod pinned to the Kubernetes master / control-plane node, collecting cluster-wide state across all namespaces with **zero external dependencies** — no Prometheus, Grafana, Redis, or databases required. It features real-time asynchronous watch streams, in-memory caching, deterministic alert evaluation, and real-time WebSocket broadcasting for the **OmniKube Dashboard**.

---

## Architecture Overview

```
                                      +-------------------------------------------------------+
                                      |             Kubernetes Cluster (Any K8s)              |
                                      |                                                       |
                                      |  +-------------------------------------------------+  |
                                      |  |          Control-Plane / Master Node            |  |
+----------------------------------+  |  |                                                 |  |
|      OmniKube Dashboard UI       |  |  |   +-----------------------------------------+   |  |
|    (React / Vite on Port 9998)   |  |  |   |             OmniKube Agent              |  |
+-----------------+----------------+  |  |   |          (FastAPI on Port 9999)         |  |
                  |                   |  |   +--------------------+--------------------+  |
                  | REST & WebSocket  |  |                        |                        |  |
                  | (Port-Forward /   |  |   +--------------------+--------------------+   |  |
                  | Ingress / Service)|  |   | - In-Memory Cache (Thread-Safe RLock)   |   |  |
                  v                   |  |   | - Alert Engine (Stable ID & Auto-Resolv)|   |  |
+----------------------------------+  |  |   | - Async Watchers & Reconciliation       |   |  |
|       Internal K8s Service       |---->|   | - WebSocket Broadcaster (/api/stream)   |   |  |
|   (k8s-monitoring-agent:9999)    |  |  |   +--------------------+--------------------+   |  |
+----------------------------------+  |  |                        |                        |  |
                                      |  |                        | In-Cluster Auth        |  |
                                      |  |                        v (Mounted ServiceAccount|  |
                                      |  |   +-----------------------------------------+   |  |
                                      |  |   |             kube-apiserver              |   |  |
                                      |  |   +--------------------+--------------------+   |  |
                                      |  +------------------------|------------------------+  |
                                      |                           |                            |
                                      |                           v                            |
                                      |          +----------------------------------+          |
                                      |          | Cluster Resources across all NS  |          |
                                      |          |  Pods, Deployments, Services,    |          |
                                      |          |  EndpointSlices, Events          |          |
                                      |          +----------------------------------+          |
                                      +-------------------------------------------------------+
```

### Core Architecture Components

1. **Authentication Boundary (`app/kubernetes_client.py`)**:
   - In production, loads in-cluster credentials via `kubernetes.config.load_incluster_config()` using the Pod's mounted `ServiceAccount` token.
   - Never falls back silently to mock data or local developer kubeconfig.
   - Cleanly handles network latency, 403 authorization failures, and API outages.

2. **In-Memory Cache (`app/cache.py`)**:
   - Thread-safe storage protected by re-entrant read/write locks (`threading.RLock`).
   - Indexes cluster state by `(namespace, name)` and tracks per-resource sync timestamps.
   - Accurate staleness detection (`cache_stale_threshold_seconds = 120s`).

3. **Collectors & Issue Detection (`app/collectors/`)**:
   - **Pods**: Parses container states (Running, Waiting, Terminated), exit codes, restart counts, and accurately detects `CrashLoopBackOff`, `ImagePullBackOff`, `ErrImagePull`, `CreateContainerError`, `ProlongedPending` (>300s), and terminating pods. Does not assume a Running pod is healthy.
   - **Deployments**: Tracks desired vs. available replicas, stale observed generations, progress failures (`ProgressDeadlineExceeded`), and handles zero-replica deployments (`desired == 0`) cleanly as healthy.
   - **Services & EndpointSlices**: Associates Services with DiscoveryV1 `EndpointSlice` resources by `kubernetes.io/service-name` label; detects selector-backed services lacking ready endpoints while preserving headless/external services.
   - **Events**: Collects recent `Normal` and `Warning` Kubernetes events for diagnostic context.

4. **Deterministic Alert Engine (`app/alerts.py`)**:
   - Evaluates rules across active cache snapshots.
   - Generates stable, deterministic alert IDs: `sha256(rule:kind:namespace:name)[:16]`.
   - Automatic deduplication (updates `lastSeen` without duplicate alert spam).
   - Auto-resolution: alerts automatically flip to `status: "resolved"` the moment the underlying issue disappears from the cluster.

5. **Async Watchers & Non-Blocking Startup (`app/watchers.py`)**:
   - Spawns background thread workers running official `watch.Watch().stream()` loops with automatic reconnect and HTTP 410 (Resource Version Gone) recovery.
   - Periodic reconciliation loop (every 60s) guaranteeing eventual consistency.
   - Non-blocking startup: initial synchronization runs in a background thread, enabling Uvicorn to bind port `9999` in under 1ms so liveness probes pass immediately.

6. **Real-Time WebSocket Broadcaster (`app/broadcaster.py`)**:
   - High-performance WebSocket hub on `/api/stream`.
   - Sends full cluster snapshot immediately upon client connection.
   - Broadcasts real-time events (`resource_updated`, `resource_deleted`, `alert_created`, `alert_resolved`, `heartbeat`).

---

## Connecting with OmniKube Dashboard

The **OmniKube Dashboard** is a standalone, modern web application that visualizes cluster telemetry, deployment health, active alerts, and real-time resource graphs.

### Connection Architecture

```
+---------------------------------------------------------------------------------------+
|                                 OmniKube Platform                                     |
|                                                                                       |
|   +------------------------------------+             +----------------------------+   |
|   |         OmniKube Dashboard         |             |       OmniKube Agent       |   |
|   |   (React / TypeScript / Vite)      |             |     (FastAPI Backend)      |   |
|   |                                    |             |                            |   |
|   |  - Real-Time WebSocket Listener    |<=== WS =====|  /api/stream               |   |
|   |  - Cluster Summary Cards           |--- REST --->|  /api/summary              |   |
|   |  - Pod & Deployment Inventories    |--- REST --->|  /api/pods, /api/deploy... |   |
|   |  - Interactive Alerts Feed         |--- REST --->|  /api/alerts               |   |
|   +------------------------------------+             +----------------------------+   |
+---------------------------------------------------------------------------------------+
```

### Connection Methods

#### 1. Local Development / Testing
Run `kubectl port-forward` on your workstation so the dashboard can access the agent on localhost:
```bash
kubectl port-forward svc/k8s-monitoring-agent 9999:9999
```
Configure your OmniKube Dashboard environment (`.env`):
```env
VITE_AGENT_API_URL=http://localhost:9999
VITE_AGENT_WS_URL=ws://localhost:9999/api/stream
```

#### 2. In-Cluster Internal Communication
If the OmniKube Dashboard runs inside the cluster, connect directly via Kubernetes internal CoreDNS:
```
http://k8s-monitoring-agent.default.svc.cluster.local:9999
```

#### 3. Production Remote Access via Ingress
Expose the agent through an Ingress controller with TLS and optional mutual TLS (mTLS) or OAuth2-Proxy:
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: omnikube-agent-ingress
  namespace: default
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
spec:
  ingressClassName: nginx
  tls:
    - hosts: [agent.omnikube.internal]
      secretName: omnikube-tls
  rules:
    - host: agent.omnikube.internal
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: k8s-monitoring-agent
                port:
                  number: 9999
```

### CORS Configuration
By default, the agent restricts CORS. To allow the dashboard frontend domain, configure the `CORS_ALLOWED_ORIGINS` environment variable in `k8s/deployment.yaml`:
```yaml
- name: CORS_ALLOWED_ORIGINS
  value: '["http://localhost:9998","https://dashboard.omnikube.internal"]'
```

---

## Runtime Execution Modes

The agent enforces three explicit, mutually exclusive operational modes via the `APP_MODE` environment variable:

| Mode | Target | Authentication | Behavior |
| :--- | :--- | :--- | :--- |
| **`production`** *(default)* | Live Kubernetes Pod | In-Cluster ServiceAccount | Queries `kube-apiserver`. If unreachable, `/readyz` returns 503. **Never** substitutes fake data. |
| **`mock`** | Local PC Development | None (Simulated Data) | Simulates multi-namespace cluster data with healthy/crashing pods, replica shortages, and events. |
| **`local-cluster`** | Local Workstation | `~/.kube/config` | Connects directly to a developer's existing Minikube, Kind, or remote cluster kubeconfig. |

---

## Local Development & Testing

Develop, run, and test the agent on your PC without needing a running Kubernetes cluster.

### 1. Prerequisites
- Python 3.10+ (Python 3.12 recommended)
- `pip` and virtualenv

### 2. Setup Virtual Environment
```bash
# Clone the repository
git clone https://github.com/Bhavesh9480/OmniKube-Agent.git
cd OmniKube-Agent

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate       # On Windows: .\.venv\Scripts\Activate.ps1

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Run Automated Unit Tests
The test suite contains **37 comprehensive tests** validating collectors, alert deduplication, auto-resolution, cache concurrency, and all REST/WebSocket endpoints offline:
```bash
pytest -v
```

### 4. Run Locally in Mock Mode
Start the agent locally with simulated multi-namespace cluster data:
```bash
APP_MODE=mock uvicorn app.main:app --host 0.0.0.0 --port 9999 --reload
```
Access the local APIs:
- Interactive Swagger UI: `http://localhost:9999/docs`
- Summary: `http://localhost:9999/api/summary`
- Health: `http://localhost:9999/api/health`

---

## Kubernetes Production Deployment

The agent is designed to run on the **master/control-plane node** with strictly read-only privileges.

### 1. Build the Container Image

#### Option A: Inside Minikube (Fastest for testing)
```bash
minikube image build -t k8s-monitoring-agent:1.0.0 .
```

#### Option B: Standard Docker / Container Registry
```bash
docker build -t k8s-monitoring-agent:1.0.0 .
docker tag k8s-monitoring-agent:1.0.0 <your-registry>/k8s-monitoring-agent:1.0.0
docker push <your-registry>/k8s-monitoring-agent:1.0.0
```

### 2. Deploy Manifests to Kubernetes
Apply the manifests located in the `k8s/` directory in order:

```bash
# 1. Create the dedicated ServiceAccount
kubectl apply -f k8s/serviceaccount.yaml

# 2. Grant least-privilege read-only permissions
kubectl apply -f k8s/rbac.yaml

# 3. Deploy the agent pinned to the control-plane node
kubectl apply -f k8s/deployment.yaml

# 4. Expose the internal ClusterIP service
kubectl apply -f k8s/service.yaml
```

### 3. Verify Deployment
```bash
# Confirm pod is scheduled on the control-plane node
kubectl get pods -l app.kubernetes.io/name=k8s-monitoring-agent -o wide

# Check startup logs
kubectl logs -l app.kubernetes.io/name=k8s-monitoring-agent -f
```

---

## Security Hardening & RBAC

### Least-Privilege Read-Only RBAC
The agent is bound to a `ClusterRole` with **only read verbs** (`get`, `list`, `watch`):
- Core: `pods`, `services`, `events`
- Apps: `deployments`
- Discovery: `endpointslices`

It has **zero write, create, patch, delete, or exec permissions**, and cannot read sensitive resources such as Secrets or ConfigMaps.

Verify RBAC rules using `kubectl auth can-i`:
```bash
# Allowed read operations (returns 'yes'):
kubectl auth can-i list pods --as=system:serviceaccount:default:k8s-monitoring-agent
kubectl auth can-i list deployments --as=system:serviceaccount:default:k8s-monitoring-agent

# Blocked write operations (returns 'no'):
kubectl auth can-i create pods --as=system:serviceaccount:default:k8s-monitoring-agent
kubectl auth can-i delete pods --as=system:serviceaccount:default:k8s-monitoring-agent
kubectl auth can-i get secrets --as=system:serviceaccount:default:k8s-monitoring-agent
```

### Container Hardening
- **Non-Root**: Runs under UID `10001` (`appuser:appgroup`).
- **Read-Only Root Filesystem**: Backed by an in-memory `emptyDir` mount for `/tmp`.
- **Privilege Escalation Disabled**: `allowPrivilegeEscalation: false`.
- **Dropped Capabilities**: `capabilities: drop: ["ALL"]`.
- **No Host Privileges**: No host network, no host PID, no host path mounts.

---

## REST & WebSocket API Specification

All endpoints run on port `9999` and return structured JSON validated via Pydantic models:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/summary` | Cluster health status, connectivity, and aggregated resource counts. |
| `GET` | `/api/pods` | Monitored pods. Query params: `namespace`, `phase`, `attention`, `limit`, `continue_token`. |
| `GET` | `/api/deployments`| Monitored deployments. Query params: `namespace`, `attention`. |
| `GET` | `/api/services` | Monitored services with associated EndpointSlice health. |
| `GET` | `/api/endpointslices`| Raw EndpointSlice network topology. |
| `GET` | `/api/events` | Recent cluster-wide diagnostic events (`Warning` / `Normal`). |
| `GET` | `/api/alerts` | Active and resolved alert feed. Query params: `severity`, `namespace`, `limit`. |
| `GET` | `/api/metrics` | Lightweight in-memory agent cache metrics. |
| `WS` | `/api/stream` | Full-duplex WebSocket streaming snapshots, live resource diffs, and heartbeats. |
| `GET` | `/healthz` | Process liveness probe (`200 OK`). |
| `GET` | `/readyz` | Service readiness probe (`200 OK` when synced, `503` while syncing or disconnected). |
| `GET` | `/api/health` | Comprehensive telemetry (cache age, watchers status, errors) without leaking secrets. |

### Sample Response: `GET /api/summary`
```json
{
  "status": "warning",
  "cluster": {
    "connected": true,
    "lastSuccessfulSync": "2026-10-26T12:00:00Z"
  },
  "pods": {
    "total": 28,
    "running": 24,
    "pending": 2,
    "failed": 1,
    "unknown": 0,
    "terminating": 1,
    "unhealthy": 4
  },
  "deployments": {
    "total": 8,
    "healthy": 6,
    "unhealthy": 2
  },
  "services": {
    "total": 10,
    "withoutReadyEndpoints": 1
  },
  "alerts": {
    "critical": 1,
    "warning": 3,
    "info": 0,
    "total": 4
  }
}
```

---

## Resource Utilization & Footprint

Live measurements taken from running clusters:

| Metric | Measured Usage | Allocation Request | Allocation Limit | Headroom |
| :--- | :--- | :--- | :--- | :--- |
| **RAM (RSS)** | **~48 MiB** | 256 MiB | 512 MiB | **> 90% Free** |
| **CPU** | **~3m (0.3% core)** | 100m | 500m | **> 99% Free** |
| **Threads** | **8 Threads** | N/A | N/A | Highly Efficient |

---

## Configuration Reference

Settings can be overridden using environment variables or a `.env` file:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `APP_MODE` | `production` | Operational mode: `production`, `mock`, `local-cluster`. |
| `PORT` | `9999` | Port to bind for HTTP and WebSocket traffic. |
| `HOST` | `0.0.0.0` | Host network interface to bind. |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `PENDING_POD_THRESHOLD_SECONDS` | `300` | Duration (seconds) before a Pending pod triggers a warning alert. |
| `CRASH_LOOP_RESTART_THRESHOLD` | `3` | Restart threshold before an alert is raised. |
| `CACHE_STALE_THRESHOLD_SECONDS` | `120` | Max cache age before readiness probe flags data as stale. |
| `RECONCILIATION_INTERVAL_SECONDS` | `60` | Full periodic reconciliation sweep interval. |
| `CORS_ALLOWED_ORIGINS` | `[]` | Allowed origins list for browser security. |

---

## License

This project is licensed under the Apache 2.0 License — see the [LICENSE](LICENSE) file for details.
