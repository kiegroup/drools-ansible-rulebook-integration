#!/usr/bin/env bash
# Usage: ./load_test_temporal_HA-PG.sh
#
# Measures HA persistence cost for a once_within rule under rapid ingress.
# 10 groups × (size/10) events per group. First event per group fires
# (10 MATCHING_EVENT rows), the rest are suppressed within a 60s window.
# HA-PG only. Requires Docker for PostgreSQL.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

SIZES=("100" "500" "1k")

OUT="result_temporal_HA-PG.txt"
LOG="out_temporal_HA-PG.log"
> "$OUT"
> "$LOG"

require_jar
trap pg_cleanup EXIT
pg_setup

# Header
{
  echo "=== Temporal once_within Load Test (HA-PG) ==="
  printf "%-32s %8s %14s %9s %14s %10s %12s\n" \
    "File" "Events" "Memory(bytes)" "Time(ms)" "Per-Event(KB)" "MATCHING" "BlobSize(B)"
  printf -- "-%.0s" {1..105}; echo
} | tee -a "$OUT"

for size in "${SIZES[@]}"; do
  pg_truncate
  file="once_within_${size}_events.json"
  events=$(size_to_int "$size")
  label="$file (HA-PG)"
  echo "Running $label..."
  jvm_run "$label" "$file" --ha-db-params "$PG_PARAMS"
  fmt_parse_metrics "$_run_stderr" "$file"
  kb=$(fmt_per_event_kb "$_mem" "$events")
  matching=$(pg_count drools_ansible_matching_event)
  blob=$(pg_blob_size)
  append_metric_line "$OUT" "$_run_stderr" "$file" "$label"
  printf "%-32s %8d %14s %9s %14s %10s %12s\n" \
    "$file" "$events" "$_mem" "$_time" "$kb" "$matching" "$blob" | tee -a "$OUT"
done

echo ""
echo "Results written to $OUT"
echo "Full logs in $LOG"
run_memory_analyzer "$OUT"
