#!/usr/bin/env bash
# Usage: ./load_test_match.sh [1k|10k|100k|1m]
#
# Runs 24kb_<size>_events.json once with noHA, once with HA-PG.
# Requires Docker for PostgreSQL.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

size="${1:-1k}"
case "$size" in
  1k|10k|100k|1m) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [1k|10k|100k|1m]"
    exit 1
    ;;
esac

file="24kb_${size}_events.json"
count_for_size() {
  case "$1" in
    1k) echo 1000 ;;
    10k) echo 10000 ;;
    100k) echo 100000 ;;
    1m) echo 1000000 ;;
  esac
}
count=$(count_for_size "$size")

OUT="result_match_${size}.txt"
LOG="out_match.log"
> "$LOG"

require_jar
trap pg_cleanup EXIT
pg_setup

{
  echo "=== Match Load Test (size=${size}) ==="
  printf "\n%-8s %-8s %14s %9s %14s\n" "Mode" "Events" "Memory(bytes)" "Time(ms)" "Per-Event(KB)"
  printf "%s\n" "$(head -c 60 < /dev/zero | tr '\0' '-')"
} | tee "$OUT"

# noHA
echo "Running $file (noHA)..."
jvm_run "$file (noHA)" "$file"
fmt_parse_metrics "$_run_stderr" "$file"
per_event=$(fmt_per_event_kb "$_mem" "$count")
printf "%-8s %-8s %14s %9s %14s\n" "noHA" "$size" "$_mem" "$_time" "$per_event" | tee -a "$OUT"

# HA-PG
echo "Running $file (HA-PG)..."
pg_truncate
jvm_run "$file (HA-PG)" "$file" --ha-db-params "$PG_PARAMS"
fmt_parse_metrics "$_run_stderr" "$file"
per_event=$(fmt_per_event_kb "$_mem" "$count")
printf "%-8s %-8s %14s %9s %14s\n" "HA-PG" "$size" "$_mem" "$_time" "$per_event" | tee -a "$OUT"

echo "Results written to $OUT"
echo "Full logs in $LOG"
