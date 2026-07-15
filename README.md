# eyebench

A small, read-heavy commerce API built as a set of async Python microservices,
with a Prometheus + Grafana observability stack for local performance testing.

## Topology

All services are Python / async (FastAPI + asyncio):

| Service          | Port | Role                                                        |
|------------------|------|-------------------------------------------------------------|
| `gateway`        | 8000 | Edge service; aggregates the three services below           |
| `product`        | 8001 | Cache-fronted read service (Redis in front of Postgres)     |
| `cart`           | 8002 | Read/write; calls `product` to enrich line items            |
| `recommendation` | 8003 | Recommendation ranking, called by the gateway               |
| `worker`         | 8004 | Background tier (recomputes trending, drains a task queue)  |
| Redis            | 6379 | Cache                                                       |
| Postgres         | 5432 | Primary store (schema + seed in `db/init.sql`)              |

The gateway is the public entry point; a request to `/home/{uid}` fans out to
the cart, recommendation, and product services and returns a combined view.

## Observability

The stack runs on Kubernetes (minikube); see [`k8s/README.md`](k8s/README.md)
for deployment. Observability lives in its own `monitoring` namespace:

- **Prometheus** (`:9090`) scrapes RED metrics per service (request rate,
  errors, latency histograms) plus operational gauges: DB pool wait-time /
  in-use, event-loop lag, cache hit-rate, queue depth, and (via
  prometheus_client defaults) RSS + GC stats. Per-pod container CPU / memory /
  network / IO come from the kubelet's cAdvisor; object state (restarts, ready,
  limits) from kube-state-metrics; the node's USE ceiling from node-exporter.
- **Loki** (`:3100`) stores logs; **Grafana Alloy** (`:12345`) tails each
  eyebench pod's logs via the Kubernetes API, parses the JSON lines, and ships
  them to Loki with `service` / `container` / `level` labels that line up with
  the metric labels.
- **Grafana** (`:3000`, anonymous viewer enabled) with Pod/Deployment Health,
  RED, USE, Pods, DB, App and Logs dashboards, and Prometheus / Loki
  datasources for Explore.

Everything is deployed from plain manifests — no kube-prometheus-stack Helm
release, no Prometheus Operator, no Alertmanager. All datastores are configured
for 10-year retention.

### Logging

Every service logs structured JSON to stdout (see
`services/common/logging_config.py`): one object per line with `time`, `level`,
`logger`, `service`, `message` plus structured fields (e.g. `method`, `path`,
`status`, `duration_ms` on the request access log). This makes queries like
`{service="cart"} | json | status>=500` work in Grafana.

## Metrics

Defined once in `services/common/metrics.py` and emitted by every service:

| Metric                             | What it tracks                         |
|------------------------------------|----------------------------------------|
| `http_request_duration_seconds`    | request latency (p50/p95/p99)          |
| `db_pool_wait_seconds`             | time spent waiting for a DB connection |
| `event_loop_lag_seconds`           | asyncio scheduling delay               |
| `cache_requests_total{result}`     | cache hits vs misses                   |
| `downstream_requests_total`        | service-to-service call rate/status    |
| `downstream_request_duration_*`    | service-to-service call latency        |
| `process_resident_memory_bytes`    | resident memory                        |
| `db_queries_total{op}`             | query counts per logical operation     |

## Running

The stack runs on Kubernetes (minikube). Full steps — build images, deploy both
namespaces, load dashboards, scale the gateway — are in
[`k8s/README.md`](k8s/README.md). In brief:

```bash
minikube start --cpus=6 --memory=8g
minikube addons enable ingress
minikube image build -t eyebench-app:latest ./services
kubectl apply -f k8s/00-namespace.yaml
kubectl -n eyebench create configmap pg-init --from-file=db/init.sql
kubectl apply -f k8s/
```

Then port-forward what you want to reach:

```bash
kubectl -n monitoring port-forward svc/grafana 3000:80      # Grafana (admin/admin)
kubectl -n monitoring port-forward svc/prometheus 9090:9090 # Prometheus
```

## Layout

```
services/
  common/         shared code (metrics, db pool, cache, downstream client, app factory)
  gateway/ product/ cart/ recommendation/ worker/
  Dockerfile      one image, launched per-service via the container command
db/init.sql       schema + seed (1000 products, 500 users, sample cart activity)
k8s/              Kubernetes manifests (app in `eyebench`, monitoring in `monitoring`)
  observability/  Grafana dashboard JSON + the Alloy config reference
.env              tunable settings
```
