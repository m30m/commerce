#!/usr/bin/env bash
# Spin up a fresh, static namespace that SERVES a snapshot taken by
# scripts/snapshot-monitoring.sh. It creates a PVC per datastore, populates each
# from the snapshot tarballs (via a throwaway loader pod), then deploys a
# self-contained Grafana + Loki + Prometheus + Pyroscope wired to those PVCs.
# The serve namespace collects nothing (no scrapers, no Alloy, Prometheus has no
# scrape_configs) — it only replays the frozen data.
#
# Usage:
#   scripts/serve-snapshot.sh <snapshot-name> <target-namespace> \
#       [--in-dir ./snapshots] [--pvc-size 10Gi] [--storage-class NAME]
#
# Teardown:  kubectl delete ns <target-namespace>
set -euo pipefail

IN_DIR="./snapshots"
PVC_SIZE="10Gi"
STORAGE_CLASS=""
SNAPSHOT_NAME=""
NS=""

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="${REPO_ROOT}/k8s/snapshot/serve.yaml.tmpl"
DASH_DIR="${REPO_ROOT}/k8s/observability"

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --in-dir)        IN_DIR="$2"; shift 2 ;;
    --pvc-size)      PVC_SIZE="$2"; shift 2 ;;
    --storage-class) STORAGE_CLASS="$2"; shift 2 ;;
    -h|--help)       usage 0 ;;
    -*)              echo "unknown flag: $1" >&2; usage 1 ;;
    *)
      if   [[ -z "$SNAPSHOT_NAME" ]]; then SNAPSHOT_NAME="$1"; shift
      elif [[ -z "$NS" ]];            then NS="$1"; shift
      else echo "unexpected argument: $1" >&2; usage 1; fi ;;
  esac
done

[[ -n "$SNAPSHOT_NAME" && -n "$NS" ]] || { echo "error: <snapshot-name> and <target-namespace> are required" >&2; usage 1; }
[[ -f "$TEMPLATE" ]] || { echo "error: template not found: $TEMPLATE" >&2; exit 1; }

SRC="${IN_DIR}/${SNAPSHOT_NAME}"
for store in loki prometheus pyroscope; do
  [[ -s "${SRC}/${store}.tar" ]] || { echo "error: missing/empty ${SRC}/${store}.tar (run snapshot-monitoring.sh first)" >&2; exit 1; }
done

# store | PVC claim name | container mount path used by the datastore
STORES=(
  "loki|loki-data|/loki"
  "prometheus|prometheus-data|/prometheus"
  "pyroscope|pyroscope-data|/data"
)

echo ">> creating namespace '${NS}'"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

# --- populate one PVC from a tarball via a throwaway loader pod --------------
load_store() {
  local store="$1" claim="$2" tar="${SRC}/$1.tar" loader="snap-loader-$1"
  local sc_line=""
  [[ -n "$STORAGE_CLASS" ]] && sc_line="  storageClassName: ${STORAGE_CLASS}"

  echo ">> ${store}: creating PVC ${claim} (${PVC_SIZE})"
  kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${claim}
  namespace: ${NS}
  labels: { app: ${store}, role: snapshot-data }
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: ${PVC_SIZE}
${sc_line}
EOF

  # Fresh loader each run (idempotent), mounting the PVC at /data.
  kubectl -n "$NS" delete pod "$loader" --ignore-not-found --wait=true >/dev/null
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${loader}
  namespace: ${NS}
  labels: { role: snapshot-loader }
spec:
  restartPolicy: Never
  containers:
    - name: loader
      image: busybox:1.37
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - { name: data, mountPath: /data }
  volumes:
    - name: data
      persistentVolumeClaim: { claimName: ${claim} }
EOF

  echo ">> ${store}: waiting for loader pod"
  kubectl -n "$NS" wait --for=condition=Ready "pod/${loader}" --timeout=180s

  echo ">> ${store}: restoring $(wc -c <"$tar" | tr -d ' ') bytes into ${claim}"
  kubectl -n "$NS" cp "$tar" "${loader}:/tmp/data.tar"
  kubectl -n "$NS" exec "$loader" -- tar xf /tmp/data.tar -C /data

  # Free the RWO PVC before the datastore Deployment claims it.
  kubectl -n "$NS" delete pod "$loader" --wait=true >/dev/null
}

for entry in "${STORES[@]}"; do
  IFS='|' read -r store claim _ <<<"$entry"
  load_store "$store" "$claim"
done

# --- deploy the static serve stack ------------------------------------------
echo ">> rendering and applying serve manifests"
if command -v envsubst >/dev/null 2>&1; then
  SNAPSHOT_NS="$NS" envsubst '${SNAPSHOT_NS}' <"$TEMPLATE" | kubectl apply -f -
else
  sed "s/\${SNAPSHOT_NS}/${NS}/g" "$TEMPLATE" | kubectl apply -f -
fi

# --- dashboards (same JSON as the live stack, keyed by datasource uid) -------
echo ">> loading dashboards ConfigMap"
kubectl -n "$NS" create configmap eyebench-dashboards \
  $(printf ' --from-file=%s' "$DASH_DIR"/*.json) \
  --dry-run=client -o yaml | kubectl apply -f -

echo ">> waiting for grafana to become ready"
kubectl -n "$NS" rollout status deploy/grafana --timeout=180s || true

cat <<EOF

>> snapshot '${SNAPSHOT_NAME}' is now served from namespace '${NS}'.

   Open Grafana (admin / admin):
     kubectl -n ${NS} port-forward svc/grafana 3000:80
   Prometheus (optional):
     kubectl -n ${NS} port-forward svc/prometheus 9090:9090

   This namespace is static: it scrapes/ships/ingests nothing.

   Tear it down (removes pods AND the snapshot PVCs):
     kubectl delete ns ${NS}
EOF
