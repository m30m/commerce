# eyebench on minikube

Kubernetes port of the compose stack, centered on scaling the **gateway** out
to multiple replicas behind an **L7 Ingress**.

## What changes vs docker-compose

| Concern | compose | here |
|---|---|---|
| Gateway parallelism | `uvicorn --workers N` (one pod, N processes) | `replicas: N` (N pods, 1 process each) |
| Metrics | one `/metrics` per container; multi-worker undercounts | `ServiceMonitor` scrapes **each pod** — correct per-replica |
| Profiling | sidecar joins `pid: service:gateway` (app is PID 1) | sidecar + `shareProcessNamespace`, discovers app PID by cmdline |
| loadgen → gateway | direct, L4 (per-connection) | via Ingress, **L7 (per-request)** balancing |

> Single-node reminder: minikube is one VM. 4 gateway replicas share that VM's
> cores — this improves *isolation, balancing, and observability*, not raw
> capacity. The node CPU (`minikube start --cpus=N`) is still the ceiling, and
> none of this removes the httpcore pool overhead (see the per-origin pool
> change in `services/common/downstream.py`).

> Run every command below **from the repo root** — paths like `k8s/…`,
> `db/init.sql`, and `./services` are relative to it. The steps are ordered;
> run them top to bottom.

## 1. Prerequisites

```bash
minikube start --cpus=6 --memory=8g
minikube addons enable ingress          # nginx ingress controller
minikube addons enable metrics-server   # needed only if you add an HPA
```

## 2. Install the monitoring stack (Prometheus + Grafana)

**Do this before deploying the app** — it installs the Prometheus Operator CRDs
(`ServiceMonitor`), so the gateway's ServiceMonitor in step 4 applies cleanly.
Deploying first means `kubectl apply -f k8s/` silently skips the ServiceMonitor
(unknown kind) and Prometheus scrapes nothing.

`kube-prom-values.yaml` wires the Loki + Pyroscope datasources, sets
`serviceMonitorSelector: {}` so Prometheus scrapes ServiceMonitors regardless of
label (the gateway's is labeled `app=gateway`, not `release=kube-prom`), and
enables the dashboard sidecar. The release name **must** be `kube-prom` — the
Prometheus Service DNS and the built-in `prometheus` datasource uid derive from it.

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install kube-prom prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  -f k8s/observability/kube-prom-values.yaml
kubectl -n monitoring rollout status deploy/kube-prom-grafana
```

## 3. Build images into the cluster

```bash
minikube image build -t eyebench-app:latest ./services
minikube image build -t eyebench-loadgen:latest ./load
```

## 4. Deploy eyebench

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl -n eyebench create configmap pg-init --from-file=db/init.sql
kubectl apply -f k8s/          # includes the gateway ServiceMonitor from step 2's CRD
kubectl -n eyebench rollout status deploy/gateway
```

## 5. Load dashboards & open Grafana

The `eyebench` namespace now exists, so the dashboard ConfigMap lands in it; the
Grafana sidecar auto-imports any ConfigMap labeled `grafana_dashboard=1`.

```bash
kubectl -n eyebench create configmap eyebench-dashboards \
  --from-file=k8s/observability/red.json \
  --from-file=k8s/observability/use.json \
  --from-file=k8s/observability/pods.json \
  --from-file=k8s/observability/logs.json \
  --from-file=k8s/observability/profiling.json
kubectl -n eyebench label configmap eyebench-dashboards grafana_dashboard=1

kubectl -n monitoring port-forward svc/kube-prom-grafana 3000:80
# browse http://localhost:3000  (user: admin)
kubectl -n monitoring get secret kube-prom-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d; echo
```

Dashboards (folder **eyebench** in Grafana): **RED** (rate/errors/duration),
**USE** (utilisation/saturation), **Pods** (per-pod CPU/memory/restarts from
cAdvisor + kube-state-metrics), **Logs** (Loki), **Profiling** (Pyroscope).

## 6. Scale the gateway

```bash
kubectl -n eyebench scale deploy/gateway --replicas=4
# per-pod CPU (each pod still ~pins one core under load):
kubectl -n eyebench top pods -l app=gateway
```

## 7. Verify the Ingress is spreading load across pods

```bash
# request counts should be roughly even across the 4 pods (L7). If one or two
# pods get all the traffic, you are accidentally hitting the Service (L4).
kubectl -n eyebench logs -l app=gateway --prefix --tail=50 | grep '"path"'
```

Or in Prometheus/Grafana:

```promql
sum by (pod) (rate(http_requests_total{service="gateway"}[1m]))
```
