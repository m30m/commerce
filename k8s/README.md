# eyebench on minikube

Kubernetes port of the stack, centered on scaling the **gateway** out to multiple
replicas behind an **L7 Ingress**, with a self-contained observability plane in a
dedicated `monitoring` namespace.

## Namespaces

| Namespace    | Contents |
|--------------|----------|
| `eyebench`   | app services (gateway ×N, product, cart, recommendation), Postgres, Redis, loadgen |
| `monitoring` | Prometheus, Grafana, kube-state-metrics, node-exporter, Loki, Pyroscope, Alloy |

The two are independent: redeploying the app never disturbs the metrics/logs/
profiles history, and vice-versa.

## What changes vs the old compose stack

| Concern | compose | here |
|---|---|---|
| Gateway parallelism | `uvicorn --workers N` (one pod, N processes) | `replicas: N` (N pods, 1 process each) |
| Metrics | one `/metrics` per container; multi-worker undercounts | Prometheus scrapes **each pod endpoint** — correct per-replica |
| Profiling | sidecar joins `pid: service:gateway` (app is PID 1) | sidecar + `shareProcessNamespace`, discovers app PID by cmdline |
| loadgen → gateway | direct, L4 (per-connection) | via Ingress, **L7 (per-request)** balancing |
| Monitoring | Prometheus/Grafana/cAdvisor containers | hand-rolled manifests (no Helm, no Prometheus Operator) |

> The monitoring stack is **plain manifests** — there is no kube-prometheus-stack
> Helm release and no ServiceMonitor/PodMonitor CRDs. Prometheus discovers its
> targets itself via `kubernetes_sd_configs` + relabeling (see
> `70-monitoring-prometheus.yaml`). There is no Alertmanager.

> All datastores (Prometheus, Loki, Pyroscope) are configured for **10-year
> retention** — a benchmark run should never age its own data out. Storage is
> still `emptyDir` (ephemeral); the retention governs aging, not durability. Swap
> in PVCs if you need the data to survive pod restarts.

> Single-node reminder: minikube is one VM. N gateway replicas share that VM's
> cores — this improves *isolation, balancing, and observability*, not raw
> capacity. The node CPU (`minikube start --cpus=N`) is still the ceiling.

> Run every command below **from the repo root** — paths like `k8s/…`,
> `db/init.sql` are relative to it. The steps are ordered; run them top to bottom.

## 1. Prerequisites

```bash
minikube start --cpus=6 --memory=8g
minikube addons enable ingress          # nginx ingress controller
minikube addons enable metrics-server   # needed only if you add an HPA
```

No Helm required.

## 2. Build images into the cluster

```bash
minikube image build -t eyebench-app:latest ./services
minikube image build -t eyebench-loadgen:latest ./load
```

## 3. Deploy everything

`kubectl apply -f k8s/` applies the top-level manifests only (it is
non-recursive, so the dashboard JSON under `k8s/observability/` is left alone —
those are loaded as a ConfigMap in step 4). The `pg-init` ConfigMap must exist
before Postgres starts, so create it first.

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl -n eyebench create configmap pg-init --from-file=db/init.sql
kubectl apply -f k8s/
kubectl -n eyebench   rollout status deploy/gateway
kubectl -n monitoring rollout status deploy/prometheus
kubectl -n monitoring rollout status deploy/grafana
```

## 4. Load the dashboards

Grafana mounts the `eyebench-dashboards` ConfigMap (optional) and re-scans every
30s, so you can create it any time — before or after Grafana starts.

```bash
kubectl -n monitoring create configmap eyebench-dashboards \
  --from-file=k8s/observability/health.json \
  --from-file=k8s/observability/red.json \
  --from-file=k8s/observability/use.json \
  --from-file=k8s/observability/pods.json \
  --from-file=k8s/observability/logs.json \
  --from-file=k8s/observability/profiling.json
```

Dashboards (folder **eyebench** in Grafana):

- **Pod / Deployment Health** — per-pod CPU / memory / network / disk IO from
  cAdvisor, with a pod selector (pick one pod, several, or a whole Deployment's
  replicas). This is the "single pane" health view.
- **RED** — rate / errors / duration (app request metrics).
- **USE** — utilisation / saturation (RSS, event-loop lag, DB pool, cache).
- **Pods** — per-pod CPU/mem/restarts/ready (cAdvisor + kube-state-metrics).
- **Logs** — Loki.
- **Profiling** — Pyroscope flame graphs.

## 5. Open Grafana

```bash
kubectl -n monitoring port-forward svc/grafana 3000:80
# browse http://localhost:3000  (anonymous viewer, or admin / admin)
```

Prometheus, if you want it directly:

```bash
kubectl -n monitoring port-forward svc/prometheus 9090:9090
```

## 6. Scale the gateway

```bash
kubectl -n eyebench scale deploy/gateway --replicas=4
# per-pod CPU (each pod still ~pins one core under load):
kubectl -n eyebench top pods -l app=gateway
```

## 7. Verify the Ingress is spreading load across pods

```bash
# request counts should be roughly even across the pods (L7). If one or two pods
# get all the traffic, you are accidentally hitting the Service (L4).
kubectl -n eyebench logs -l app=gateway --prefix --tail=50 | grep '"path"'
```

Or in Prometheus/Grafana:

```promql
sum by (pod) (rate(http_requests_total{service="gateway"}[1m]))
```
