#!/usr/bin/env bash
# Start (or stop) the single local SeaweedFS server used by the ASV benchmarks.
#
# One server hosts every benchmark in the ASV run; benchmark processes reach it on
# localhost (see python/benchmarks/seaweed_utils.py for the client-side helpers).
#
# Automatic vacuuming is disabled (-master.garbageThreshold=1.1 can never be reached),
# so space is reclaimed only by the benchmarks themselves, by dropping a bucket's
# backing collection.
#
# Volumes are capped at 2GB and never preallocated, so a high -volume.max only reserves
# slots, not disk. The slots matter because every bucket is grown to at least as many
# volumes as ArcticDB has IO threads.

set -euo pipefail

SEAWEED_VERSION="${SEAWEED_VERSION:-4.40}"
SEAWEED_DATA_DIR="${SEAWEED_DATA_DIR:-${RUNNER_TEMP:-/tmp}/seaweedfs-asv}"
SEAWEED_BIN_DIR="${SEAWEED_BIN_DIR:-/usr/local/bin}"
SEAWEED_IP="${SEAWEED_IP:-127.0.0.1}"
SEAWEED_MASTER_PORT="${SEAWEED_MASTER_PORT:-9333}"
SEAWEED_VOLUME_PORT="${SEAWEED_VOLUME_PORT:-8080}"
SEAWEED_FILER_PORT="${SEAWEED_FILER_PORT:-8888}"
SEAWEED_S3_PORT="${SEAWEED_S3_PORT:-8333}"
SEAWEED_VOLUME_SIZE_LIMIT_MB="${SEAWEED_VOLUME_SIZE_LIMIT_MB:-2048}"
SEAWEED_MAX_VOLUMES="${SEAWEED_MAX_VOLUMES:-1000}"

PID_FILE="$SEAWEED_DATA_DIR/weed.pid"
LOG_FILE="$SEAWEED_DATA_DIR/weed.log"

download() {
    if [[ -x "$SEAWEED_BIN_DIR/weed" ]]; then
        return
    fi
    echo "Installing SeaweedFS $SEAWEED_VERSION"
    mkdir -p "$SEAWEED_BIN_DIR"
    local installer="$SEAWEED_BIN_DIR/install.sh"
    curl -fsSL --retry 3 -o "$installer" https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/install.sh
    bash "$installer" --version "$SEAWEED_VERSION" --dir "$SEAWEED_BIN_DIR"
    rm -f "$installer"
}

# Everything needed to tell why a probe failed, without a local SeaweedFS to reproduce against.
# Each endpoint's status and (truncated) body are printed, so a probe that fails because the
# response shape changed is distinguishable from one that fails because nothing is listening.
diagnostics() {
    echo "--- SeaweedFS diagnostics ---" >&2
    echo "[version] $("$SEAWEED_BIN_DIR/weed" version 2>&1 | head -1)" >&2
    echo "[data dir] $SEAWEED_DATA_DIR (log: $LOG_FILE)" >&2
    local name path body
    for probe in \
        "master:$SEAWEED_MASTER_PORT/dir/status" \
        "master-volumes:$SEAWEED_MASTER_PORT/vol/status" \
        "s3-root:$SEAWEED_S3_PORT/" \
        "filer:$SEAWEED_FILER_PORT/" \
        "volume:$SEAWEED_VOLUME_PORT/status"; do
        name="${probe%%:*}"
        path="${probe#*:}"
        body=$(curl -sS -m 10 -w '<http_code:%{http_code}>' "http://$SEAWEED_IP:$path" 2>&1 || true)
        echo "[$name] http://$SEAWEED_IP:$path -> ${body:0:1500}" >&2
    done
    echo "[listening] $( (ss -ltn 2>/dev/null || netstat -ltn 2>/dev/null || true) |
        grep -E ":($SEAWEED_MASTER_PORT|$SEAWEED_VOLUME_PORT|$SEAWEED_FILER_PORT|$SEAWEED_S3_PORT)\b" | tr '\n' ';')" >&2
    echo "[log errors]" >&2
    grep -E "^E" "$LOG_FILE" 2>/dev/null | tail -20 >&2 || true
    echo "[log tail]" >&2
    tail -50 "$LOG_FILE" >&2 2>/dev/null || true
}

start() {
    # A foreign server answering here would also answer the readiness probe below, making this
    # script report success while its own process dies on the port bind
    if curl -fsS "http://$SEAWEED_IP:$SEAWEED_MASTER_PORT/dir/status" >/dev/null 2>&1; then
        echo "Something is already answering on http://$SEAWEED_IP:$SEAWEED_MASTER_PORT;" \
            "refusing to start a second SeaweedFS. Stop it first (or override SEAWEED_*_PORT)." >&2
        exit 1
    fi
    download
    mkdir -p "$SEAWEED_DATA_DIR"
    # AWS_* creds must not leak into weed: with them set the S3 gateway creates an admin
    # identity from them and enables auth, rejecting the benchmarks' placeholder credentials
    env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
        nohup "$SEAWEED_BIN_DIR/weed" server \
        -dir="$SEAWEED_DATA_DIR" \
        -ip="$SEAWEED_IP" \
        -ip.bind="$SEAWEED_IP" \
        -master.port="$SEAWEED_MASTER_PORT" \
        -master.volumeSizeLimitMB="$SEAWEED_VOLUME_SIZE_LIMIT_MB" \
        -master.volumePreallocate=false \
        -master.garbageThreshold=1.1 \
        -volume.port="$SEAWEED_VOLUME_PORT" \
        -volume.max="$SEAWEED_MAX_VOLUMES" \
        -filer \
        -filer.port="$SEAWEED_FILER_PORT" \
        -s3 \
        -s3.port="$SEAWEED_S3_PORT" \
        -s3.port.iceberg=0 \
        >"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"

    # Ready when the master is up, a volume server has registered and the S3 gateway answers
    for _ in $(seq 1 120); do
        if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            rm -f "$PID_FILE"
            echo "SeaweedFS process died on startup" >&2
            diagnostics
            exit 1
        fi
        # S3 must answer 2xx: with auth disabled anonymous ListBuckets succeeds, so anything
        # else (e.g. 403) means auth got enabled and the benchmarks' credentials would be rejected
        master_code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' "http://$SEAWEED_IP:$SEAWEED_MASTER_PORT/dir/status" 2>/dev/null || true)
        s3_code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' "http://$SEAWEED_IP:$SEAWEED_S3_PORT" 2>/dev/null || true)
        if [[ "$master_code" == 2* && "$s3_code" == 2* ]]; then
                    echo "SeaweedFS $SEAWEED_VERSION is up (master HTTP $master_code, s3 HTTP $s3_code). IP: $SEAWEED_IP, PID: $(cat $PID_FILE) master port :$SEAWEED_MASTER_PORT, filer port :$SEAWEED_FILER_PORT," \
                "s3 port :$SEAWEED_S3_PORT, data in $SEAWEED_DATA_DIR"
            return
        fi
        sleep 1
    done
    echo "SeaweedFS did not become ready in time" \
        "(last master HTTP code: ${master_code:-none}, last s3 HTTP code: ${s3_code:-none})" >&2
    diagnostics
    exit 1
}

stop() {
    tail -20 "$LOG_FILE" 2>/dev/null || true
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        kill "$(cat "$PID_FILE")"
        for _ in $(seq 1 30); do
            kill -0 "$(cat "$PID_FILE")" 2>/dev/null || break
            sleep 1
        done
        kill -9 "$(cat "$PID_FILE")" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    echo "SeaweedFS stopped (data left in $SEAWEED_DATA_DIR)"
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    status) diagnostics ;;
    *)
        echo "Usage: $0 [start|stop|status]" >&2
        exit 1
        ;;
esac
