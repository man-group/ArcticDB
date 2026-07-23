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
            echo "SeaweedFS process died on startup, last log lines:" >&2
            tail -50 "$LOG_FILE" >&2
            exit 1
        fi
        if curl -fsS "http://$SEAWEED_IP:$SEAWEED_MASTER_PORT/dir/status" 2>/dev/null | grep -q '"DataNodes"' &&
            curl -fsS "http://$SEAWEED_IP:$SEAWEED_S3_PORT" >/dev/null 2>&1; then
                    echo "SeaweedFS $SEAWEED_VERSION is up. IP: $SEAWEED_IP, PID: $(cat $PID_FILE) master port :$SEAWEED_MASTER_PORT, filer port :$SEAWEED_FILER_PORT," \
                "s3 port :$SEAWEED_S3_PORT, data in $SEAWEED_DATA_DIR"
            return
        fi
        sleep 1
    done
    echo "SeaweedFS failed to start, last log lines:" >&2
    tail -50 "$LOG_FILE" >&2
    exit 1
}

stop() {
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
    *)
        echo "Usage: $0 [start|stop]" >&2
        exit 1
        ;;
esac
