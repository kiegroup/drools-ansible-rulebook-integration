#!/usr/bin/env bash

# Usage: ./load_test_failover.sh [100|500|1k]
#
# Measures HA failover recovery time under load with PostgreSQL.
# Uses a 2-condition join rule so partial matches accumulate in the DB.
# Phase 1 (Node1): processes events into PG (building DB state), then exits.
# Phase 2 (Node2): starts fresh, enableLeader() recovers state from PG.
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

# 2) Validate it
case "$size" in
  100|500|1k) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [100|500|1k]"
    exit 1
    ;;
esac

# 3) Prepare filename
test_file="failover_${size}_events.json"

out="result_failover_${size}.txt"
LOG="out_failover.log"
> "$LOG"

# 4) Docker PostgreSQL setup
PG_CONTAINER=""
PG_PARAMS=""

setup_postgres() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not available. PostgreSQL is required for failover test."
    exit 1
  fi

  local pg_port
  pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

  echo "Starting PostgreSQL container on port $pg_port..."
  PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_USER=failovertest \
    -e POSTGRES_PASSWORD=failovertest \
    -e POSTGRES_DB=failovertest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U failovertest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "ERROR: PostgreSQL failed to start within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  # pg_isready can return OK before authentication is fully ready; verify with an actual query
  local conn_retries=10
  while ! docker exec "$PG_CONTAINER" psql -U failovertest -d failovertest -c "SELECT 1" >/dev/null 2>&1; do
    conn_retries=$((conn_retries - 1))
    if [ "$conn_retries" -le 0 ]; then
      echo "ERROR: PostgreSQL authentication not ready within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"failovertest\",\"user\":\"failovertest\",\"password\":\"failovertest\",\"sslmode\":\"disable\"}"
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

# 5) Generate a shared HA UUID for this test run
HA_UUID="failover-loadtest-$(date +%s)-$$"

# 6) Helper: run java, capture stderr for metrics
# Usage: run_java <label> <args...>
# Sets: _run_stderr
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

# Helper: extract metrics from stderr
# Usage: parse_metrics "$stderr_output" "$file"
# Sets: _mem and _time
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

# 7) Header
header=$(printf "=== Failover Recovery Load Test (size=%s) ===" "$size")
table_header=$(printf "\n%-36s %-14s %14s %9s" "File" "Phase" "Memory(bytes)" "Time(ms)")
separator=$(printf "%s" "$(head -c 75 < /dev/zero | tr '\0' '-')")

{
  echo "$header"
  echo "$table_header"
  echo "$separator"
} | tee "$out"

# 8) Phase 1: Load events into PG (Node1)
echo ""
echo "Phase 1: Loading events into PostgreSQL ($test_file)..."
run_java "$test_file (node1-load)" "$test_file" \
  --ha-db-params "$PG_PARAMS" \
  --ha-uuid "$HA_UUID"
parse_metrics "$_run_stderr" "$test_file"
load_mem="$_mem"; load_time="$_time"

# 9) Phase 2: Failover recovery (Node2)
echo "Phase 2: Failover recovery ($test_file)..."
run_java "$test_file (node2-recovery)" "$test_file" \
  --ha-db-params "$PG_PARAMS" \
  --ha-uuid "$HA_UUID" \
  --failover-recovery
parse_metrics "$_run_stderr" "$test_file"
recovery_mem="$_mem"; recovery_time="$_time"

# 10) Calculate ratio
ratio="N/A"
if [ "$load_time" != "FAILED" ] && [ "$recovery_time" != "FAILED" ] && [ "$load_time" -gt 0 ] 2>/dev/null; then
  ratio=$(awk "BEGIN { printf \"%.1f\", $recovery_time / $load_time * 100 }")
fi

# 11) Print results
{
  printf "%-36s %-14s %14s %9s\n" "$test_file" "Load(PG)" "$load_mem" "$load_time"
  printf "%-36s %-14s %14s %9s\n" "$test_file" "Recovery(PG)" "$recovery_mem" "$recovery_time"
  printf "%-36s %-14s %14s %8s%%\n" "" "Ratio" "" "$ratio"
  echo ""
} | tee -a "$out"

echo "Results written to $out"
echo "Full logs in $LOG"
