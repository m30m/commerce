# eyebench

A small, read-heavy commerce API built as a set of async Python microservices,
with a Prometheus + Grafana observability stack and a load generator for local
performance testing.

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

- **Prometheus** (`:9090`) scrapes RED metrics per service (request rate,
  errors, latency histograms) plus operational gauges: DB pool wait-time /
  in-use, event-loop lag, cache hit-rate, queue depth, and (via
  prometheus_client defaults) RSS + GC stats.
- **Loki** (`:3100`) stores logs; **Grafana Alloy** (`:12345`) reads each
  container's logs from the Docker socket, parses the JSON lines, and ships
  them to Loki with `service` / `container` / `level` labels that line up with
  the metric labels.
- **Grafana** (`:3000`, anonymous viewer enabled) with RED, USE and Logs
  dashboards, and both Prometheus and Loki datasources for Explore.

  cAdvisor v0.49.1 does not support Docker's containerd image-store
  snapshotter (`google/cadvisor#3643`, still open) — with it enabled,
  cAdvisor fails to resolve each container's read-write layer and reports no
  container metrics at all. If `docker info` shows
  `driver-type: io.containerd.snapshotter.v1`, disable it on the host via
  `/etc/docker/daemon.json`:

  ```json
  {
    "features": {
      "containerd-snapshotter": false
    }
  }
  ```

  then `systemctl restart docker` and `docker compose up -d` to recreate the
  stack under the classic `overlay2` graphdriver. This is a host-level Docker
  daemon setting, not something `docker-compose.yml` can fix — it restarts
  every container on the host.

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

## Load generator

`load/generator.py` drives traffic against the gateway. It uses an open-loop
(constant arrival rate) model so throughput measurements aren't distorted when
the system is under pressure, draws keys from a Zipfian distribution so hot-key
and cache-eviction effects show up, and runs a read-heavy (~90/10) mix. Tune it
via `.env` (`RPS`, `ZIPF_S`, `DURATION_S`, ...). It logs client-observed
p50/p95/p99 every 10s.

## Running

```bash
docker compose up --build
```

Then:

- Gateway:      http://localhost:8000/home/1
- Prometheus:   http://localhost:9090
- Grafana:      http://localhost:3000  (anonymous viewer, or admin/admin)
- Logs:         Grafana → Dashboards → Logs, or Explore with the Loki datasource
- Load output:  `docker compose logs -f loadgen`

Tune the arrival rate in `.env` (`RPS`), and set `DURATION_S` to run the load
generator for a fixed duration instead of indefinitely.

## Layout

```
services/
  common/         shared code (metrics, db pool, cache, downstream client, app factory)
  gateway/ product/ cart/ recommendation/ worker/
  Dockerfile      one image, launched per-service via compose command
db/init.sql       schema + seed (1000 products, 500 users, sample cart activity)
load/             load generator
observability/    prometheus, loki, alloy configs + grafana provisioning & dashboards
docker-compose.yml
.env              tunable settings
```
