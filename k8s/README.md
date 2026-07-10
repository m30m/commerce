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

## Prerequisites

```bash
minikube start --cpus=6 --memory=8g
minikube addons enable ingress          # nginx ingress controller
minikube addons enable metrics-server   # needed only if you add an HPA
# ServiceMonitor needs the Prometheus Operator CRDs, e.g.:
#   helm install kube-prom prometheus-community/kube-prometheus-stack -n monitoring --create-namespace
# (skip 30-gateway's ServiceMonitor if you are not running the operator)
```

## Build images into the cluster

```bash
minikube image build -t eyebench-app:latest ./services
minikube image build -t eyebench-loadgen:latest ./load
```

## Deploy

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl -n eyebench create configmap pg-init --from-file=db/init.sql
kubectl apply -f k8s/
kubectl -n eyebench rollout status deploy/gateway
```

## Scale the gateway

```bash
kubectl -n eyebench scale deploy/gateway --replicas=4
# per-pod CPU (each pod still ~pins one core under load):
kubectl -n eyebench top pods -l app=gateway
```

## Verify the Ingress is spreading load across pods

```bash
# request counts should be roughly even across the 4 pods (L7). If one or two
# pods get all the traffic, you are accidentally hitting the Service (L4).
kubectl -n eyebench logs -l app=gateway --prefix --tail=50 | grep '"path"'
```

Or in Prometheus/Grafana:

```promql
sum by (pod) (rate(http_requests_total{service="gateway"}[1m]))
```
