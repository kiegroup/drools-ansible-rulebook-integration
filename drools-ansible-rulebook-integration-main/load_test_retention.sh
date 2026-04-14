#!/usr/bin/env bash

# Usage: ./load_test_retention.sh
#
# Measures memory growth when retaining large events as partial matches.
# Uses failover_XX_events.json (2-condition join rule, only condition 1 satisfied).
# All events stay in working memory as partial matches — never matched, never discarded.
#
# Runs 100, 500, 1k events in both noHA and HA(PG) modes.
# Reports memory and per-event memory estimate for each.
# Requires Docker for PostgreSQL.

set -euo pipefail

JAR="target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Fat JAR not found at $JAR"
  echo "Run: mvn -pl drools-ansible-rulebook-integration-main -am package -DskipTests"
  exit 1
fi

SIZES=(100 500 1k)
FILES=("failover_100_events.json" "failover_500_events.json" "failover_1k_events.json")
COUNTS=(100 500 1000)

OUT="result_retention.txt"
LOG="out_retention.log"
> "$LOG"

# Docker PostgreSQL setup
PG_CONTAINER=""
PG_PARAMS=""

setup_postgres() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not available. PostgreSQL is required."
    exit 1
  fi

  local pg_port
  pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

  echo "Starting PostgreSQL container on port $pg_port..."
  PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_USER=retentiontest \
    -e POSTGRES_PASSWORD=retentiontest \
    -e POSTGRES_DB=retentiontest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U retentiontest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "ERROR: PostgreSQL failed to start within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  local conn_retries=10
  while ! docker exec "$PG_CONTAINER" psql -U retentiontest -d retentiontest -c "SELECT 1" >/dev/null 2>&1; do
    conn_retries=$((conn_retries - 1))
    if [ "$conn_retries" -le 0 ]; then
      echo "ERROR: PostgreSQL authentication not ready within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"retentiontest\",\"user\":\"retentiontest\",\"password\":\"retentiontest\",\"sslmode\":\"disable\"}"
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

# Helper: run java, capture stderr for metrics
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

# Helper: count rows in a PG table. Returns "ERR" on failure.
pg_count() {
  local table="$1"
  docker exec "$PG_CONTAINER" psql -U retentiontest -d retentiontest -tAc "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "ERR"
}

# Helper: max length (bytes) of the partial_matching_events TEXT column in SESSION_STATE.
pg_blob_size() {
  docker exec "$PG_CONTAINER" psql -U retentiontest -d retentiontest -tAc \
    "SELECT COALESCE(MAX(length(partial_matching_events)), 0) FROM drools_ansible_session_state" 2>/dev/null || echo "ERR"
}

# Helper: truncate all HA tables so each HA-PG run starts clean.
pg_truncate() {
  docker exec "$PG_CONTAINER" psql -U retentiontest -d retentiontest -c \
    "TRUNCATE drools_ansible_session_state, drools_ansible_matching_event, drools_ansible_action_info, drools_ansible_ha_stats" >/dev/null 2>&1 || true
}

# Helper: extract metrics from stderr
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

# Header
{
  echo "=== Event Retention Memory Analysis ==="
  echo "Event payload: ~24KB JSON each (2-condition join, all retained as partial matches)"
  echo ""
  printf "%-10s %-8s %14s %9s %14s %10s %14s\n" \
    "Mode" "Events" "Memory(bytes)" "Time(ms)" "Per-Event(KB)" "MATCHING" "BlobSize(B)"
  printf "%s\n" "$(head -c 88 < /dev/zero | tr '\0' '-')"
} | tee "$OUT"

# Associative arrays to store results for delta calculation
declare -A MEM_RESULTS

for run_mode in noHA HA-PG; do
  for idx in 0 1 2; do
    file="${FILES[$idx]}"
    count="${COUNTS[$idx]}"
    size="${SIZES[$idx]}"

    echo "Running $file ($run_mode)..."

    if [ "$run_mode" = "noHA" ]; then
      run_java "$file ($run_mode)" "$file"
      matching_rows="-"
      blob_size="-"
    else
      pg_truncate
      run_java "$file ($run_mode)" "$file" --ha-db-params "$PG_PARAMS"
      matching_rows=$(pg_count "drools_ansible_matching_event")
      blob_size=$(pg_blob_size)
    fi

    parse_metrics "$_run_stderr" "$file"
    mem="$_mem"
    time="$_time"

    # Calculate per-event memory (KB)
    per_event="N/A"
    if [ "$mem" != "FAILED" ] && [ "$mem" -gt 0 ] 2>/dev/null; then
      per_event=$(awk "BEGIN { printf \"%.1f\", $mem / $count / 1024 }")
    fi

    printf "%-10s %-8s %14s %9s %14s %10s %14s\n" \
      "$run_mode" "$size" "$mem" "$time" "$per_event" "$matching_rows" "$blob_size" | tee -a "$OUT"

    # Store for delta calculation
    MEM_RESULTS["${run_mode}_${idx}"]="$mem"
  done
  echo "" | tee -a "$OUT"
done

# Delta analysis: incremental per-event cost between sizes
{
  echo "=== Incremental Per-Event Cost (delta between sizes) ==="
  echo ""
  printf "%-10s %-12s %14s %14s\n" "Mode" "Range" "Delta(bytes)" "Per-Event(KB)"
  printf "%s\n" "$(head -c 54 < /dev/zero | tr '\0' '-')"

  for run_mode in noHA HA-PG; do
    for pair in "0:1:100-500:400" "1:2:500-1k:500"; do
      IFS=':' read -r from_idx to_idx label delta_count <<< "$pair"
      from_mem="${MEM_RESULTS["${run_mode}_${from_idx}"]}"
      to_mem="${MEM_RESULTS["${run_mode}_${to_idx}"]}"

      if [ "$from_mem" != "FAILED" ] && [ "$to_mem" != "FAILED" ] 2>/dev/null; then
        delta=$((to_mem - from_mem))
        per_event_delta=$(awk "BEGIN { printf \"%.1f\", $delta / $delta_count / 1024 }")
        printf "%-10s %-12s %14s %14s\n" "$run_mode" "$label" "$delta" "$per_event_delta"
      else
        printf "%-10s %-12s %14s %14s\n" "$run_mode" "$label" "FAILED" "N/A"
      fi
    done
  done

  # HA overhead delta (HA-PG minus noHA)
  echo ""
  echo "--- HA overhead (HA-PG minus noHA) ---"
  printf "%-12s %14s %14s\n" "Range" "Delta(bytes)" "Per-Event(KB)"
  printf "%s\n" "$(head -c 42 < /dev/zero | tr '\0' '-')"

  for pair in "0:100:100" "1:500:500" "2:1k:1000"; do
    IFS=':' read -r idx label count <<< "$pair"
    noha_mem="${MEM_RESULTS["noHA_${idx}"]}"
    ha_mem="${MEM_RESULTS["HA-PG_${idx}"]}"

    if [ "$noha_mem" != "FAILED" ] && [ "$ha_mem" != "FAILED" ] 2>/dev/null; then
      overhead=$((ha_mem - noha_mem))
      per_event_overhead=$(awk "BEGIN { printf \"%.1f\", $overhead / $count / 1024 }")
      printf "%-12s %14s %14s\n" "$label" "$overhead" "$per_event_overhead"
    else
      printf "%-12s %14s %14s\n" "$label" "FAILED" "N/A"
    fi
  done

  echo ""
  echo "--- Notes ---"
  echo "Incremental per-event cost eliminates JVM baseline overhead."
  echo "  noHA delta   = M (Java Map in Drools working memory)"
  echo "  HA-PG delta  = M + J (Map + JSON string in EventRecord)"
  echo "  HA overhead  = J (the EventRecord JSON copy)"
  echo ""
} | tee -a "$OUT"

echo "Results written to $OUT"
echo "Full logs in $LOG"
