#!/usr/bin/env bash
# Usage: ./load_test_failover_HA-PG.sh
#
# Measures HA failover recovery time across two JVM runs sharing one PG:
#   Phase 1 (Load):     --ha-db-params $PG_PARAMS --ha-uuid $HA_UUID
#   Phase 2 (Recovery): + --failover-recovery, times engine.enableLeader()
# Uses retention_{size}_events.json only as a fixture to seed recoverable HA state.
# Analyzer input is restricted to failover recovery runs so this script focuses on failover behavior.
# HA-PG only. Requires Docker for PostgreSQL.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

SIZES=("100" "500" "1k")

OUT="result_failover_HA-PG.txt"
LOG="out_failover_HA-PG.log"
> "$OUT"
> "$LOG"

require_jar
trap pg_cleanup EXIT
pg_setup

# Header
{
  echo "=== Failover Recovery Load Test (HA-PG) ==="
  printf "%-34s %-14s %14s %9s\n" "File" "Phase" "Memory(bytes)" "Time(ms)"
  printf -- "-%.0s" {1..75}; echo
} | tee -a "$OUT"

declare -A LOAD_MS RECOVERY_MS

for size in "${SIZES[@]}"; do
  pg_truncate  # baseline per size; NOT between Phase 1 and Phase 2
  file="retention_${size}_events.json"
  HA_UUID="failover-loadtest-${size}-$(date +%s%N)"

  # Phase 1: Load events into PG under $HA_UUID
  label="$file (load)"
  echo "Running $label..."
  jvm_run "$label" "$file" --ha-db-params "$PG_PARAMS" --ha-uuid "$HA_UUID"
  fmt_parse_metrics "$_run_stderr" "$file"
  LOAD_MS[$size]="$_time"
  printf "%-34s %-14s %14s %9s\n" "$file" "Load(PG)" "$_mem" "$_time" | tee -a "$OUT"

  # Phase 2: Cold-start Node2, time enableLeader() recovery against the same $HA_UUID
  label="$file (recovery)"
  echo "Running $label..."
  jvm_run "$label" "$file" --ha-db-params "$PG_PARAMS" --ha-uuid "$HA_UUID" --failover-recovery
  fmt_parse_metrics "$_run_stderr" "$file"
  append_metric_line "$OUT" "$_run_stderr" "$file" "$label"
  RECOVERY_MS[$size]="$_time"
  printf "%-34s %-14s %14s %9s\n" "$file" "Recovery(PG)" "$_mem" "$_time" | tee -a "$OUT"
done

# Ratio table
{
  echo ""
  echo "=== Recovery Ratio ==="
  printf "%-6s %10s %14s %8s\n" "Size" "Load(ms)" "Recovery(ms)" "Ratio"
  printf -- "-%.0s" {1..42}; echo
  for size in "${SIZES[@]}"; do
    lm="${LOAD_MS[$size]}"
    rm_val="${RECOVERY_MS[$size]}"
    ratio="N/A"
    if [ "$lm" != "FAILED" ] && [ "$rm_val" != "FAILED" ] && [ "$lm" -gt 0 ] 2>/dev/null; then
      ratio=$(awk "BEGIN { printf \"%.1f%%\", $rm_val / $lm * 100 }")
    fi
    printf "%-6s %10s %14s %8s\n" "$size" "$lm" "$rm_val" "$ratio"
  done
} | tee -a "$OUT"

echo ""
echo "Results written to $OUT"
echo "Full logs in $LOG"
run_memory_analyzer "$OUT"
