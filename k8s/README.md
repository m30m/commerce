# eyebench on minikube

Kubernetes deployment of the stack: the app scales the **gateway** out to
multiple replicas behind an **L7 Ingress**, with a self-contained observability
plane in a dedicated `monitoring` namespace.

| Namespace    | Contents |
|--------------|----------|
| `eyebench`   | app services (gateway ×N, product, cart, recommendation), Postgres, Redis, loadgen |
| `monitoring` | Prometheus, Grafana, kube-state-metrics, node-exporter, Loki, Pyroscope, Alloy |

The monitoring stack is plain manifests — no kube-prometheus-stack Helm release,
no Prometheus Operator (Prometheus discovers targets via `kubernetes_sd_configs`
+ relabeling), no Alertmanager. All datastores use 10-year retention on
`emptyDir`, so data ages out only after 10y but does not survive a pod restart —
swap in PVCs if you need durability.

Run all commands **from the repo root**.

## 1. Prerequisites

```bash
minikube start --cpus=6 --memory=8g
minikube addons enable ingress          # nginx ingress controller
minikube addons enable metrics-server   # needed only if you add an HPA
```

## 2. Build images

```bash
minikube image build -t eyebench-app:latest ./services
minikube image build -t eyebench-loadgen:latest ./load
```

## 3. Deploy

`kubectl apply -f k8s/` is non-recursive, so it applies the top-level manifests
and leaves the dashboard JSON under `k8s/observability/` alone. Create the
`pg-init` ConfigMap first — Postgres mounts it at startup.

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl -n eyebench create configmap pg-init --from-file=db/init.sql
kubectl apply -f k8s/
kubectl -n eyebench   rollout status deploy/gateway
kubectl -n monitoring rollout status deploy/grafana
```

## 4. Load the dashboards

Grafana mounts this ConfigMap (optional) and re-scans every 30s, so create it
any time.

```bash
kubectl -n monitoring create configmap eyebench-dashboards \
  --from-file=k8s/observability/health.json \
  --from-file=k8s/observability/node.json \
  --from-file=k8s/observability/red.json \
  --from-file=k8s/observability/use.json \
  --from-file=k8s/observability/pods.json \
  --from-file=k8s/observability/logs.json \
  --from-file=k8s/observability/profiling.json
```

To apply dashboard edits later, recreate it in place:

```bash
kubectl -n monitoring create configmap eyebench-dashboards \
  $(printf ' --from-file=%s' k8s/observability/*.json) \
  --dry-run=client -o yaml | kubectl apply -f -
```

Dashboards (folder **eyebench** in Grafana):

- **Pod / Deployment Health** — per-pod CPU / memory / disk IO with a pod
  selector (one pod, several, or a whole Deployment's replicas). Network on this
  view is node-wide: minikube's cAdvisor exposes no per-pod network series.
- **Node** — node-wide CPU (per state: idle/system/user/iowait/…), memory,
  disk IO, filesystem usage, and network per interface (node-exporter).
- **RED** — rate / errors / duration. **USE** — utilisation / saturation.
- **Pods** — per-pod CPU/mem/restarts/ready. **Logs** — Loki.
  **Profiling** — Pyroscope flame graphs.

## 5. Open Grafana

```bash
kubectl -n monitoring port-forward svc/grafana 3000:80      # admin / admin
kubectl -n monitoring port-forward svc/prometheus 9090:9090 # Prometheus, if wanted
```

## 6. Scale the gateway

```bash
kubectl -n eyebench scale deploy/gateway --replicas=4
kubectl -n eyebench top pods -l app=gateway
```

Check the Ingress spreads load evenly across replicas (uneven = you're hitting
the L4 Service, not the Ingress):

```promql
sum by (pod) (rate(http_requests_total{service="gateway"}[1m]))
```

## 7. Snapshot a run and serve it statically

Freeze the metrics/logs/profiles from a benchmark run and re-open them later in a
throwaway namespace, without keeping the live stack scraping. Two scripts, run
from the repo root:

```bash
# 1. Capture: tar each datastore's data dir out of the running monitoring pods
#    into ./snapshots/<name>/ (best-effort live copy — the source is untouched;
#    take it after load has stopped for the cleanest cut).
scripts/snapshot-monitoring.sh run1

# 2. Serve: stand up a fresh namespace that only *serves* that snapshot.
scripts/serve-snapshot.sh run1 mon-run1

kubectl -n mon-run1 port-forward svc/grafana 3000:80   # admin / admin
```

The serve namespace (`mon-run1` above) is a trimmed clone of `monitoring`
(Grafana + Loki + Prometheus + Pyroscope) that **collects nothing**: Prometheus
runs with no `scrape_configs`, and kube-state-metrics / node-exporter / Alloy are
omitted. Each datastore mounts a PVC that the serve script pre-populates from the
snapshot tarballs, so the data is durable and the dashboards work unchanged.

Snapshots are independent: different `<name>` and target namespace can coexist.
Tear one down (pods **and** its PVCs) with:

```bash
kubectl delete ns mon-run1
```

Notes:

- `snapshot-monitoring.sh` runs `tar` inside each datastore container when the
  image ships it (Loki, Prometheus — no residue on the source pod). For the
  distroless Pyroscope image it falls back to a short-lived, non-privileged
  `kubectl debug` ephemeral container that tars the data via `/proc/1/root`; it
  self-terminates and leaves the source app running.
- Flags: `--src-namespace` / `--out-dir` (capture); `--in-dir` / `--pvc-size` /
  `--storage-class` (serve). The serve manifests are
  `k8s/snapshot/serve.yaml.tmpl` (only `${SNAPSHOT_NS}` is substituted).
- Snapshot tarballs land under `./snapshots/` (git-ignored).
