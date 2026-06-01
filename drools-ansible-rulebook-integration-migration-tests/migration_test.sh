#!/usr/bin/env bash
# Migration compatibility test driver.
#
# Default mode (no args): tests all versioned jars against latest.
# Manual mode (2 args):   tests a specific <from-version> -> <to-version> pair.
#
# Usage:
#   ./migration_test.sh                          # all versions -> latest
#   ./migration_test.sh 2.0.0-beta2 latest       # single pair

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

LOG="$SCRIPT_DIR/out_migration.log"
> "$LOG"

THIN_JAR=$(find_thin_jar "$SCRIPT_DIR")

run_migration_pair() {
  local from_version="$1"
  local to_version="$2"
  local from_jar
  from_jar=$(find_versioned_jar "$SCRIPT_DIR/versioned-jar/$from_version") || return 1
  local to_jar
  to_jar=$(find_versioned_jar "$SCRIPT_DIR/versioned-jar/$to_version") || return 1

  pg_truncate
  local ha_uuid
  ha_uuid="mig-$(uuidgen 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"

  local persist_result="OK"
  local verify_result="OK"
  local overall="PASS"

  if ! migration_run "persist ($from_version)" "$from_jar" "$THIN_JAR" \
      --persist --ha-db-params "$PG_PARAMS" --ha-uuid "$ha_uuid"; then
    persist_result="FAIL"
    overall="FAIL"
  fi

  if [ "$persist_result" = "OK" ]; then
    if ! migration_run "verify ($to_version)" "$to_jar" "$THIN_JAR" \
        --verify --ha-db-params "$PG_PARAMS" --ha-uuid "$ha_uuid"; then
      verify_result="FAIL"
      overall="FAIL"
    fi
  else
    verify_result="SKIP"
  fi

  printf "%-30s %-10s %-10s %-10s\n" "$from_version" "$persist_result" "$verify_result" "$overall"
  [ "$overall" = "PASS" ] && return 0 || return 1
}

# --- main -------------------------------------------------------------------

trap pg_cleanup EXIT
pg_setup

if [ $# -eq 2 ]; then
  # Manual mode: specific pair
  FROM_VERSION="$1"
  TO_VERSION="$2"
  echo "=== Migration Test: $FROM_VERSION -> $TO_VERSION ==="
  printf "%-30s %-10s %-10s %-10s\n" "From Version" "Persist" "Verify" "Result"
  printf "%s\n" "$(head -c 62 < /dev/zero | tr '\0' '-')"
  if run_migration_pair "$FROM_VERSION" "$TO_VERSION"; then
    echo ""
    echo "=== Result: PASS ==="
  else
    echo ""
    echo "=== Result: FAIL ==="
    echo "Full logs in $LOG"
    exit 1
  fi
else
  # Default mode: all versions -> latest
  populate_latest "$SCRIPT_DIR"

  VERSIONS=($(discover_versions "$SCRIPT_DIR"))
  if [ ${#VERSIONS[@]} -eq 0 ]; then
    echo "ERROR: No versioned jars found in versioned-jar/. Nothing to test." >&2
    exit 1
  fi

  echo "=== Migration Tests: all versions -> latest ==="
  printf "%-30s %-10s %-10s %-10s\n" "From Version" "Persist" "Verify" "Result"
  printf "%s\n" "$(head -c 62 < /dev/zero | tr '\0' '-')"

  PASSED=0
  FAILED=0
  for version in "${VERSIONS[@]}"; do
    if run_migration_pair "$version" "latest"; then
      PASSED=$((PASSED + 1))
    else
      FAILED=$((FAILED + 1))
    fi
  done

  TOTAL=$((PASSED + FAILED))
  echo ""
  echo "=== Summary: $PASSED/$TOTAL passed, $FAILED failed ==="
  echo "Full logs in $LOG"

  [ "$FAILED" -eq 0 ] || exit 1
fi
