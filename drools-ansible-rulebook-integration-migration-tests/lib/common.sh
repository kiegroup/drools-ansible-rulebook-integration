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

MAIN_CLASS="org.drools.ansible.rulebook.integration.migrationtests.MigrationTestMain"

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

# find_thin_jar <script_dir>
# Locates the migration-tests thin jar in target/.
find_thin_jar() {
  local script_dir="$1"
  local thin_jar
  thin_jar=$(ls "$script_dir"/target/drools-ansible-rulebook-integration-migration-tests-*-SNAPSHOT.jar 2>/dev/null | grep -v '\-sources\.jar' | grep -v '\-tests\.jar' | grep -v '\-javadoc\.jar' | head -1)
  if [ -z "$thin_jar" ]; then
    echo "ERROR: Migration-tests thin jar not found in target/" >&2
    echo "Build with: mvn -pl drools-ansible-rulebook-integration-migration-tests -am package -DskipTests" >&2
    exit 1
  fi
  echo "$thin_jar"
}

# find_versioned_jar <dir>
# Finds the single jar file in a versioned-jar/<version>/ directory.
find_versioned_jar() {
  local dir="$1"
  local jar
  jar=$(ls "$dir"/*.jar 2>/dev/null | head -1)
  if [ -z "$jar" ]; then
    echo "ERROR: No jar found in $dir" >&2
    exit 1
  fi
  echo "$jar"
}

populate_latest() {
  local script_dir="$1"
  local runtime_dir
  runtime_dir=$(dirname "$script_dir")/drools-ansible-rulebook-integration-runtime/target
  local src
  src=$(ls "$runtime_dir"/drools-ansible-rulebook-integration-runtime-*-HA.jar 2>/dev/null | grep -v original | head -1)
  if [ -z "$src" ]; then
    echo "ERROR: Runtime HA jar not found in $runtime_dir" >&2
    echo "Build with: mvn -pl drools-ansible-rulebook-integration-runtime -am package -DskipTests" >&2
    exit 1
  fi
  local dest_dir="$script_dir/versioned-jar/latest"
  mkdir -p "$dest_dir"
  cp "$src" "$dest_dir/"
  echo "Copied $(basename "$src") to $dest_dir/"
}

discover_versions() {
  local script_dir="$1"
  local versions=()
  for dir in "$script_dir"/versioned-jar/*/; do
    [ -d "$dir" ] || continue
    local name
    name=$(basename "$dir")
    [ "$name" = "latest" ] && continue
    if ls "$dir"/*.jar >/dev/null 2>&1; then
      versions+=("$name")
    fi
  done
  echo "${versions[@]}"
}

# ---- migration_run ---------------------------------------------------------

# migration_run <label> <ha_jar_path> <thin_jar_path> <args...>
# Runs MigrationTestMain with the HA uber-jar providing runtime classes
# and the thin jar providing MigrationTestMain + resources.
# Captures stdout to $LOG, stderr to $_run_stderr, returns exit code.
migration_run() {
  local label="$1"; shift
  local ha_jar="$1"; shift
  local thin_jar="$1"; shift
  local tmpstderr
  tmpstderr=$(mktemp)
  echo "=== $label ===" >> "$LOG"
  local rc=0
  java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -cp "$thin_jar:$ha_jar" "$MAIN_CLASS" "$@" >> "$LOG" 2>"$tmpstderr" || rc=$?
  cat "$tmpstderr" >> "$LOG"
  _run_stderr=$(cat "$tmpstderr")
  rm -f "$tmpstderr"
  echo "" >> "$LOG"
  return $rc
}
