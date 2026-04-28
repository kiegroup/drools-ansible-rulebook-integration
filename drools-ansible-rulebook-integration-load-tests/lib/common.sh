#!/usr/bin/env bash
# Shared helpers for drools-ansible-rulebook-integration-load-tests scripts.
# Source from a load_test_*.sh script:
#
#   set -euo pipefail
#   source "$(dirname "$0")/lib/common.sh"
#
# Functions are prefixed by subsystem: require_*, pg_*, jvm_*, fmt_*.

JAR="${JAR:-target/drools-ansible-rulebook-integration-load-tests-jar-with-dependencies.jar}"
PG_CONTAINER=""
PG_PARAMS=""
_run_stderr=""
_mem=""
_time=""

# ---- require_* -------------------------------------------------------------

require_jar() {
  if [ ! -f "$JAR" ]; then
    echo "ERROR: Fat JAR not found at $JAR" >&2
    echo "Build it with:" >&2
    echo "  mvn -pl drools-ansible-rulebook-integration-load-tests -am package -DskipTests" >&2
    exit 1
  fi
}

require_docker() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not available. PostgreSQL is required." >&2
    exit 1
  fi
}

require_python3() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 is required for free-port discovery." >&2
    exit 1
  fi
}

# ---- pg_* ------------------------------------------------------------------

pg_setup() {
  require_docker
  require_python3
  local pg_port
  pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
  echo "Starting PostgreSQL container on port $pg_port..."
  PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_USER=loadtest \
    -e POSTGRES_PASSWORD=loadtest \
    -e POSTGRES_DB=loadtest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U loadtest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "ERROR: PostgreSQL failed to start within timeout." >&2
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  local conn_retries=10
  while ! docker exec "$PG_CONTAINER" psql -U loadtest -d loadtest -c "SELECT 1" >/dev/null 2>&1; do
    conn_retries=$((conn_retries - 1))
    if [ "$conn_retries" -le 0 ]; then
      echo "ERROR: PostgreSQL authentication not ready within timeout." >&2
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"loadtest\",\"user\":\"loadtest\",\"password\":\"loadtest\",\"sslmode\":\"disable\"}"
  echo "PostgreSQL ready on port $pg_port"
}

pg_cleanup() {
  if [ -n "$PG_CONTAINER" ]; then
    echo "Stopping PostgreSQL container..."
    docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
    PG_CONTAINER=""
  fi
}

pg_truncate() {
  docker exec "$PG_CONTAINER" psql -U loadtest -d loadtest -c \
    "TRUNCATE drools_ansible_session_state, drools_ansible_matching_event, drools_ansible_action_info, drools_ansible_ha_stats" >/dev/null 2>&1 || true
}

pg_count() {
  local table="$1"
  docker exec "$PG_CONTAINER" psql -U loadtest -d loadtest -tAc "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "ERR"
}

pg_blob_size() {
  docker exec "$PG_CONTAINER" psql -U loadtest -d loadtest -tAc \
    "SELECT COALESCE(MAX(length(partial_matching_events)), 0) FROM drools_ansible_session_state" 2>/dev/null || echo "ERR"
}

# ---- jvm_* -----------------------------------------------------------------

# jvm_run <label> <args...>
# Runs the fat jar with -Xmx1g, appends stdout+stderr to $LOG, captures stderr into $_run_stderr.
# Tolerates non-zero JVM exit — caller inspects _run_stderr / fmt_parse_metrics.
jvm_run() {
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

# ---- fmt_* -----------------------------------------------------------------

# fmt_parse_metrics <stderr> <file>
# Finds the last stderr line beginning with <file> and splits on ",".
# Sets $_mem and $_time (or "FAILED"/"FAILED" if the line wasn't emitted).
fmt_parse_metrics() {
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

append_metric_line() {
  local out_file="$1"
  local stderr_output="$2"
  local filename="$3"
  local label="${4:-$filename}"
  local metrics_line
  metrics_line=$(echo "$stderr_output" | grep "^${filename}" | tail -1)
  if [ -z "$metrics_line" ]; then
    echo "${label}, FAILED, FAILED" >> "$out_file"
  else
    echo "$metrics_line" >> "$out_file"
  fi
}

run_memory_analyzer() {
  local result_file="$1"
  shift || true
  local analyzer_cp="target/classes:${JAR}"
  echo "Running MemoryLeakAnalyzer..."
  java -cp "$analyzer_cp" \
       org.drools.ansible.rulebook.integration.loadtests.analyze.MemoryLeakAnalyzer "$@" "$result_file"
}

# fmt_per_event_kb <bytes> <count> -> prints "%.1f" KB or "N/A".
fmt_per_event_kb() {
  local bytes="$1"
  local count="$2"
  if [ "$bytes" = "FAILED" ] || [ "$count" -le 0 ] 2>/dev/null; then
    echo "N/A"
    return
  fi
  awk "BEGIN { printf \"%.1f\", $bytes / $count / 1024 }"
}

# size_to_int <100|500|1k>
# Echoes the integer event count for a retention/temporal size label.
# Prints "ERR" and returns non-zero for an unknown label.
size_to_int() {
  case "$1" in
    100)  echo 100 ;;
    500)  echo 500 ;;
    1k)   echo 1000 ;;
    *)    echo "ERR"; return 1 ;;
  esac
}
