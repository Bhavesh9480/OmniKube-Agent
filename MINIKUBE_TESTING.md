# Testing with Minikube on Linux

This guide walks you through testing the **Kubernetes Monitoring Agent** on a Linux machine using **Minikube**.

You can test the agent in two complementary ways:
1. **Approach 1 (Local Testing via Kubeconfig)**: Run the Python agent directly on your Linux host while pointing it at Minikube (`APP_MODE=local-cluster`). Ideal for fast debugging and development.
2. **Approach 2 (In-Cluster Pod Deployment)**: Build the container image into Minikube and deploy the agent as an in-cluster Pod on the control-plane node (`APP_MODE=production`).

---

## 1. Prerequisites on Linux

Ensure the following tools are installed on your Linux machine (e.g., Ubuntu/Debian):

```bash
# Verify Docker
docker --version

# Verify Minikube
minikube version

# Verify kubectl
kubectl version --client

# Verify Python
python3 --version
```

If Minikube is not yet installed:
```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64
```

---

## 2. Start Minikube

Start your Minikube cluster using the Docker driver:

```bash
minikube start --driver=docker
```

Verify that the cluster and control-plane node are ready:
```bash
kubectl get nodes
```
*Output will show node `minikube` with role `control-plane`.*

---

## 3. Approach 1: Fast Local Testing Against Minikube (`APP_MODE=local-cluster`)

Minikube automatically creates and updates your local `~/.kube/config`. You can run the agent locally on Linux and connect directly to Minikube's API server.

### Step 1: Install Dependencies
```bash
# In the project directory
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Step 2: Run Automated Tests
```bash
pytest -v
```

### Step 3: Run Agent in `local-cluster` Mode
```bash
APP_MODE=local-cluster uvicorn app.main:app --host 0.0.0.0 --port 9999 --reload
```

In another terminal, verify the agent is reading live data from Minikube:
```bash
# Check connectivity & health
curl -s http://127.0.0.1:9999/api/health | jq .

# Check summary of live cluster resources (kube-system pods, coredns, etc.)
curl -s http://127.0.0.1:9999/api/summary | jq .

# View live pods across all namespaces
curl -s http://127.0.0.1:9999/api/pods | jq .
```

---

## 4. Approach 2: Full In-Cluster Deployment on Minikube (`APP_MODE=production`)

This tests the exact production flow: building the container image, mounting ServiceAccount credentials, RBAC authorization, and control-plane node placement.

### Step 1: Build the Container Image for Minikube

You do **not** need an external container registry with Minikube. Choose either method:

**Method A (Direct to Minikube's Docker Daemon — Fastest)**:
```bash
# Point your shell's Docker client to Minikube's Docker daemon
eval $(minikube docker-env)

# Build the image directly inside Minikube
docker build -t k8s-monitoring-agent:1.0.0 .
```

**Method B (Standard Docker Build + Image Load)**:
```bash
# Build with local Docker
docker build -t k8s-monitoring-agent:1.0.0 .

# Load the image directly into Minikube
minikube image load k8s-monitoring-agent:1.0.0
```

### Step 2: Apply Kubernetes Manifests

Apply the manifests in the `k8s/` directory:

```bash
# 1. Create the dedicated ServiceAccount
kubectl apply -f k8s/serviceaccount.yaml

# 2. Grant read-only cluster permissions
kubectl apply -f k8s/rbac.yaml

# 3. Deploy the agent to the control-plane node
kubectl apply -f k8s/deployment.yaml

# 4. Expose the internal service on port 9999
kubectl apply -f k8s/service.yaml
```

### Step 3: Verify Pod Scheduling and Startup

Check that the pod is scheduled on the control-plane node and reaches `Running` state:

```bash
kubectl get pods -l app.kubernetes.io/name=k8s-monitoring-agent -o wide
```

View the agent's startup logs and initial sync:
```bash
kubectl logs -l app.kubernetes.io/name=k8s-monitoring-agent -f
```

You should see log lines indicating:
- `Successfully loaded in-cluster Kubernetes configuration.`
- `Executing initial cluster synchronization.`
- `Cluster reconciliation completed successfully.`

### Step 4: Access the Agent API via Port-Forward

Forward port `9999` from the Kubernetes Service to your local machine:

```bash
kubectl port-forward svc/k8s-monitoring-agent 9999:9999
```

Test the endpoints from another terminal:

```bash
# 1. Liveness check
curl -s http://127.0.0.1:9999/healthz

# 2. Readiness check
curl -s http://127.0.0.1:9999/readyz

# 3. Detailed diagnostic health
curl -s http://127.0.0.1:9999/api/health | jq .

# 4. Cluster-wide summary
curl -s http://127.0.0.1:9999/api/summary | jq .

# 5. Live Pod inventory
curl -s http://127.0.0.1:9999/api/pods | jq .

# 6. Deployments
curl -s http://127.0.0.1:9999/api/deployments | jq .

# 7. Active Alerts
curl -s http://127.0.0.1:9999/api/alerts | jq .
```

---

## 5. Live Failure Testing (Simulating Incidents)

To verify the agent's real-time detection, trigger actual failure scenarios in Minikube:

### Test 1: CrashLoopBackOff Pod
```bash
# Create a pod that immediately fails
kubectl run crash-test --image=busybox --restart=Always -- /bin/sh -c "exit 1"
```
Wait ~20 seconds for Kubernetes to enter `CrashLoopBackOff`, then query:
```bash
curl -s http://127.0.0.1:9999/api/alerts | jq .
```
*The agent will immediately report a `critical` alert for `crash-test`.*

Clean up the crash pod:
```bash
kubectl delete pod crash-test
```
*Check `/api/alerts` again: the alert automatically transitions to `status: resolved`.*

---

### Test 2: ImagePullBackOff Pod
```bash
kubectl run image-fail --image=nonexistent-registry.org/fake-image:latest
```
Query:
```bash
curl -s "http://127.0.0.1:9999/api/pods?attention=true" | jq .
```
*The agent detects `ImagePullBackOff` and surfaces the failure diagnostic message.*

Clean up:
```bash
kubectl delete pod image-fail
```

---

### Test 3: Deployment Replica Shortage
```bash
# Create a deployment with 3 replicas
kubectl create deployment nginx-test --image=nginx:alpine --replicas=3

# Artificially trigger an unready state by patching with an invalid command
kubectl patch deployment nginx-test -p '{"spec":{"template":{"spec":{"containers":[{"name":"nginx","command":["nonexistentcommand"]}]}}}}'
```
Query:
```bash
curl -s http://127.0.0.1:9999/api/deployments?attention=true | jq .
curl -s http://127.0.0.1:9999/api/alerts?severity=warning | jq .
```
*The agent detects replica shortages and generates a warning alert.*

Clean up:
```bash
kubectl delete deployment nginx-test
```

---

## 6. Teardown / Cleanup

When finished testing on Minikube:

```bash
# Delete monitoring agent resources
kubectl delete -f k8s/service.yaml
kubectl delete -f k8s/deployment.yaml
kubectl delete -f k8s/rbac.yaml
kubectl delete -f k8s/serviceaccount.yaml

# Stop or delete Minikube cluster
minikube stop
# or to completely remove: minikube delete
```
