#!/usr/bin/env bash

# Usage: ./load_test_temporal.sh [100|500|1k]
#
# Measures HA persistence cost for temporal-operator rules under rapid ingress.
# Uses a once_within rule with a 60s window grouped by event.meta.uuid, so
# every event lands in its own pending window. No window expires during the
# test, so MATCHING_EVENT rows accumulate.
# Single-node, PostgreSQL only (no failover phase).
# Reports load time, memory, and final DB row counts for
# SESSION_STATE / EVENT_RECORD / MATCHING_EVENT / ACTION_INFO.
#
# Requires Docker for PostgreSQL.

set -euo pipefail

JAR="target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Fat JAR not found at $JAR"
  echo "Run: mvn -pl drools-ansible-rulebook-integration-main -am package"
  exit 1
fi

# 1) Pick up the size argument, default to "100"
size="${1:-100}"

case "$size" in
  100|500|1k) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [100|500|1k]"
    exit 1
    ;;
esac

test_file="once_within_${size}_events.json"

out="result_temporal_${size}.txt"
LOG="out_temporal.log"
> "$LOG"

# 2) Docker PostgreSQL setup
PG_CONTAINER=""
PG_PARAMS=""

setup_postgres() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not available. PostgreSQL is required for temporal test."
    exit 1
  fi

  local pg_port
  pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

  echo "Starting PostgreSQL container on port $pg_port..."
  PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_USER=temporaltest \
    -e POSTGRES_PASSWORD=temporaltest \
    -e POSTGRES_DB=temporaltest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U temporaltest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "ERROR: PostgreSQL failed to start within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  local conn_retries=10
  while ! docker exec "$PG_CONTAINER" psql -U temporaltest -d temporaltest -c "SELECT 1" >/dev/null 2>&1; do
    conn_retries=$((conn_retries - 1))
    if [ "$conn_retries" -le 0 ]; then
      echo "ERROR: PostgreSQL authentication not ready within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"temporaltest\",\"user\":\"temporaltest\",\"password\":\"temporaltest\",\"sslmode\":\"disable\"}"
  echo "PostgreSQL ready on port $pg_port"
}

cleanup_postgres() {
  if [ -n "$PG_CONTAINER" ]; then
    echo "Stopping PostgreSQL container..."
    docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
    PG_CONTAINER=""
  fi
}

trap cleanup_postgres EXIT

setup_postgres

# 3) Helper: run java, capture stderr for metrics
run_java() {
  local label="$1"; shift
  local tmpstderr
  tmpstderr=$(mktemp)
  echo "=== $label ===" >> "$LOG"
  java -Xmx1g -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar "$JAR" "$@" >> "$LOG" 2>"$tmpstderr" || true
  cat "$tmpstderr" >> "$LOG"
  _run_stderr=$(cat "$tmpstderr")
  rm -f "$tmpstderr"
  echo "" >> "$LOG"
}

parse_metrics() {
  local stderr_output="$1"
  local filename="$2"
  local metrics_line
  metrics_line=$(echo "$stderr_output" | grep "^${filename}" | tail -1)
  if [ -z "$metrics_line" ]; then
    _mem="FAILED"
    _time="FAILED"
  else
    _mem=$(echo "$metrics_line" | cut -d',' -f2 | tr -d ' ')
    _time=$(echo "$metrics_line" | cut -d',' -f3 | tr -d ' ')
  fi
}

# Helper: count rows in a PostgreSQL table. Returns "ERR" on failure.
pg_count() {
  local table="$1"
  local count
  count=$(docker exec "$PG_CONTAINER" psql -U temporaltest -d temporaltest -tAc "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "ERR")
  echo "$count"
}

pg_blob_size() {
  docker exec "$PG_CONTAINER" psql -U temporaltest -d temporaltest -tAc \
    "SELECT COALESCE(MAX(length(partial_matching_events)), 0) FROM drools_ansible_session_state" 2>/dev/null || echo "ERR"
}

# 4) Header
header=$(printf "=== Temporal Operator Load Test (once_within, size=%s) ===" "$size")
table_header=$(printf "\n%-30s %14s %9s %10s %10s %10s %10s %14s" "File" "Memory(bytes)" "Time(ms)" "SESSION" "EVENT_REC" "MATCHING" "ACTION" "BlobSize(B)")
separator=$(printf "%s" "$(head -c 120 < /dev/zero | tr '\0' '-')")

{
  echo "$header"
  echo "$table_header"
  echo "$separator"
} | tee "$out"

# 5) Load events into PG
echo ""
echo "Loading events into PostgreSQL ($test_file)..."
run_java "$test_file (HA-PG load)" "$test_file" \
  --ha-db-params "$PG_PARAMS"
parse_metrics "$_run_stderr" "$test_file"
load_mem="$_mem"; load_time="$_time"

# 6) Query final DB row counts
session_rows=$(pg_count "drools_ansible_session_state")
event_record_rows=$(pg_count "drools_ansible_event_record")
matching_rows=$(pg_count "drools_ansible_matching_event")
action_rows=$(pg_count "drools_ansible_action_info")
blob_size=$(pg_blob_size)

# 7) Print results
{
  printf "%-30s %14s %9s %10s %10s %10s %10s %14s\n" \
    "$test_file" "$load_mem" "$load_time" "$session_rows" "$event_record_rows" "$matching_rows" "$action_rows" "$blob_size"
  echo ""
} | tee -a "$out"

echo "Results written to $out"
echo "Full logs in $LOG"
