#!/usr/bin/env bash
# Freeze the monitoring plane: tar each datastore's data directory out of its
# running pod into portable per-store tarballs. Best-effort live copy — the
# source stack is left untouched (no flush, no restart). Take the snapshot after
# load has stopped for the cleanest result.
#
# Usage:
#   scripts/snapshot-monitoring.sh <snapshot-name> [--src-namespace monitoring] [--out-dir ./snapshots]
#
# Produces:
#   <out-dir>/<snapshot-name>/{loki,prometheus}.tar
#   <out-dir>/<snapshot-name>/manifest.txt
#
# Restore/serve a snapshot with scripts/serve-snapshot.sh.
set -euo pipefail

SRC_NS="monitoring"
OUT_DIR="./snapshots"
SNAPSHOT_NAME=""

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src-namespace) SRC_NS="$2"; shift 2 ;;
    --out-dir)       OUT_DIR="$2"; shift 2 ;;
    -h|--help)       usage 0 ;;
    -*)              echo "unknown flag: $1" >&2; usage 1 ;;
    *)
      if [[ -z "$SNAPSHOT_NAME" ]]; then SNAPSHOT_NAME="$1"; shift
      else echo "unexpected argument: $1" >&2; usage 1; fi ;;
  esac
done

[[ -n "$SNAPSHOT_NAME" ]] || { echo "error: <snapshot-name> is required" >&2; usage 1; }

# store | pod label | data root inside the container
STORES=(
  "loki|app=loki|/loki"
  "prometheus|app=prometheus|/prometheus"
)

DEST="${OUT_DIR}/${SNAPSHOT_NAME}"
mkdir -p "$DEST"

echo ">> snapshotting namespace '${SRC_NS}' -> ${DEST}"

# tar exits 1 when a file "changed as we read it" — expected on a live
# directory, so treat rc 0 and 1 as success and anything else as failure.
check_tar_rc() {
  local rc=$1 store=$2
  if [[ $rc -ne 0 && $rc -ne 1 ]]; then
    echo "error: tar of ${store} failed (exit ${rc})" >&2
    exit "$rc"
  fi
  if [[ $rc -eq 1 ]]; then
    echo "   (note: tar reported files changed during read — expected on a live copy)"
  fi
  return 0
}

# Capture a store's data dir into $out. Fast path: run tar inside the container
# (works when the image ships tar, e.g. Loki/Prometheus — no residue on the
# source pod). Fallback for tar-less/distroless images: attach a short-lived
# busybox ephemeral container that joins the target container's namespace and
# tars its filesystem via /proc/1/root. The ephemeral container is in-pod and
# non-privileged; it lingers (Terminated) in the pod's status until the pod
# restarts — the source app is unaffected.
capture_store() {
  local store="$1" pod="$2" root="$3" out="$4" rc

  if kubectl -n "$SRC_NS" exec "$pod" -- tar --version >/dev/null 2>&1; then
    echo ">> ${store}: tar ${root} from pod ${pod} (in-container)"
    set +e
    kubectl -n "$SRC_NS" exec "$pod" -- tar cf - -C "$root" . >"$out"
    rc=$?
    set -e
    check_tar_rc "$rc" "$store"
    return
  fi

  local ec="snapshot-$$-$(date +%s)"
  echo ">> ${store}: image has no tar; capturing ${root} via ephemeral container ${ec}"
  kubectl -n "$SRC_NS" debug "$pod" --image=busybox:1.37 \
    --target="$store" --container="$ec" -- sleep 600 >/dev/null

  local i started=""
  for i in $(seq 1 60); do
    started="$(kubectl -n "$SRC_NS" get pod "$pod" \
      -o jsonpath="{.status.ephemeralContainerStatuses[?(@.name=='${ec}')].state.running.startedAt}" 2>/dev/null || true)"
    [[ -n "$started" ]] && break
    sleep 1
  done
  [[ -n "$started" ]] || { echo "error: ephemeral container ${ec} never became ready" >&2; exit 1; }

  # Targeting a container joins its PID namespace, so the app is PID 1 there and
  # /proc/1/root is its filesystem root.
  set +e
  kubectl -n "$SRC_NS" exec "$pod" -c "$ec" -- tar cf - -C "/proc/1/root${root}" . >"$out"
  rc=$?
  set -e
  check_tar_rc "$rc" "$store"

  # Best-effort: stop the ephemeral container's sleep so it doesn't idle for
  # 10 minutes (this kills the busybox sleep, not the target app at PID 1).
  kubectl -n "$SRC_NS" exec "$pod" -c "$ec" -- sh -c 'kill $(pgrep -x sleep)' >/dev/null 2>&1 || true
}

for entry in "${STORES[@]}"; do
  IFS='|' read -r store label root <<<"$entry"

  pod="$(kubectl -n "$SRC_NS" get pod -l "$label" \
           --field-selector=status.phase=Running \
           -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    echo "error: no Running pod with label '${label}' in namespace '${SRC_NS}'" >&2
    exit 1
  fi

  out="${DEST}/${store}.tar"
  capture_store "$store" "$pod" "$root" "$out"

  if [[ ! -s "$out" ]]; then
    echo "error: ${out} is empty — nothing captured for ${store}" >&2
    exit 1
  fi
done

# Manifest: what/when/where + sizes, for provenance.
{
  echo "snapshot:      ${SNAPSHOT_NAME}"
  echo "created:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "source_ns:     ${SRC_NS}"
  echo "kube_context:  $(kubectl config current-context 2>/dev/null || echo unknown)"
  echo "files:"
  for entry in "${STORES[@]}"; do
    IFS='|' read -r store _ _ <<<"$entry"
    f="${DEST}/${store}.tar"
    echo "  ${store}.tar  $(wc -c <"$f" | tr -d ' ') bytes"
  done
} >"${DEST}/manifest.txt"

echo ">> done. snapshot written to ${DEST}"
cat "${DEST}/manifest.txt"
echo ">> serve it with:  scripts/serve-snapshot.sh ${SNAPSHOT_NAME} <target-namespace>"
