#!/usr/bin/env bash
# Shared helpers for migration test scripts.
# Source from migration_test.sh:
#
#   set -euo pipefail
#   SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#   source "$SCRIPT_DIR/lib/common.sh"

PG_CONTAINER=""
PG_PARAMS=""
LOG=""
_run_stderr=""

# ---- require_* -------------------------------------------------------------

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

require_jar() {
  local jar_path="$1"
  if [ ! -f "$jar_path" ]; then
    echo "ERROR: Jar not found at $jar_path" >&2
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
    -e POSTGRES_USER=migtest \
    -e POSTGRES_PASSWORD=migtest \
    -e POSTGRES_DB=migtest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U migtest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "ERROR: PostgreSQL failed to start within timeout." >&2
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  local conn_retries=10
  while ! docker exec "$PG_CONTAINER" psql -U migtest -d migtest -c "SELECT 1" >/dev/null 2>&1; do
    conn_retries=$((conn_retries - 1))
    if [ "$conn_retries" -le 0 ]; then
      echo "ERROR: PostgreSQL authentication not ready within timeout." >&2
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"migtest\",\"user\":\"migtest\",\"password\":\"migtest\",\"sslmode\":\"disable\"}"
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
  docker exec "$PG_CONTAINER" psql -U migtest -d migtest -c \
    "TRUNCATE drools_ansible_session_state, drools_ansible_matching_event, drools_ansible_action_info, drools_ansible_ha_stats" >/dev/null 2>&1 || true
}

pg_count() {
  local table="$1"
  docker exec "$PG_CONTAINER" psql -U migtest -d migtest -tAc "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "ERR"
}

# ---- jar management --------------------------------------------------------

populate_latest() {
  local script_dir="$1"
  local src="$script_dir/target/migration-tests-jar-with-dependencies.jar"
  local dest_dir="$script_dir/versioned-jar/latest"
  if [ ! -f "$src" ]; then
    echo "ERROR: Build output not found at $src" >&2
    echo "Build with: mvn -pl drools-ansible-rulebook-integration-migration-tests -am package -DskipTests" >&2
    exit 1
  fi
  mkdir -p "$dest_dir"
  cp "$src" "$dest_dir/migration-tests-jar-with-dependencies.jar"
  echo "Copied latest jar to $dest_dir/"
}

discover_versions() {
  local script_dir="$1"
  local versions=()
  for dir in "$script_dir"/versioned-jar/*/; do
    [ -d "$dir" ] || continue
    local name
    name=$(basename "$dir")
    [ "$name" = "latest" ] && continue
    if ls "$dir"/*-jar-with-dependencies.jar >/dev/null 2>&1; then
      versions+=("$name")
    fi
  done
  echo "${versions[@]}"
}

# ---- migration_run ---------------------------------------------------------

# migration_run <label> <jar_path> <mode> <extra_args...>
# Runs: java -Xmx512m -jar <jar> <mode> <args>
# Captures stdout to $LOG, stderr to $_run_stderr, returns exit code.
migration_run() {
  local label="$1"; shift
  local jar_path="$1"; shift
  local tmpstderr
  tmpstderr=$(mktemp)
  echo "=== $label ===" >> "$LOG"
  local rc=0
  java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar "$jar_path" "$@" >> "$LOG" 2>"$tmpstderr" || rc=$?
  cat "$tmpstderr" >> "$LOG"
  _run_stderr=$(cat "$tmpstderr")
  rm -f "$tmpstderr"
  echo "" >> "$LOG"
  return $rc
}
