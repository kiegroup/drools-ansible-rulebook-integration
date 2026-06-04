#!/usr/bin/env bash
# Usage: ./load_test_retention_noHA_HA-PG.sh
#
# Measures memory growth when events accumulate as partial matches.
# Uses retention_<N>_events.json (2-condition join, only condition 1 satisfied).
# Runs 100, 500, 1000 events in noHA and HA-PG modes.
# Requires Docker for PostgreSQL.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

SIZES=("100" "500" "1k")
FILES=("retention_100_events.json" "retention_500_events.json" "retention_1k_events.json")
COUNTS=(100 500 1000)

OUT="result_retention_noHA_HA-PG.txt"
LOG="out_retention_noHA_HA-PG.log"
> "$LOG"

require_jar
trap pg_cleanup EXIT
pg_setup

{
  echo "=== Event Retention Memory Analysis ==="
  echo "Event payload: ~24KB JSON each (2-condition join, all retained as partial matches)"
  echo ""
  printf "%-10s %-8s %14s %9s %14s %10s %14s\n" \
    "Mode" "Events" "Memory(bytes)" "Time(ms)" "Per-Event(KB)" "MATCHING" "BlobSize(B)"
  printf "%s\n" "$(head -c 88 < /dev/zero | tr '\0' '-')"
} | tee "$OUT"

declare -A MEM_RESULTS

for run_mode in noHA HA-PG; do
  for idx in 0 1 2; do
    file="${FILES[$idx]}"
    count="${COUNTS[$idx]}"
    size="${SIZES[$idx]}"

    echo "Running $file ($run_mode)..."

    if [ "$run_mode" = "noHA" ]; then
      jvm_run "$file ($run_mode)" "$file"
      matching_rows="-"
      blob_size="-"
    else
      pg_truncate
      jvm_run "$file ($run_mode)" "$file" --ha-db-params "$PG_PARAMS"
      matching_rows=$(pg_count "drools_ansible_matching_event")
      blob_size=$(pg_blob_size)
    fi

    fmt_parse_metrics "$_run_stderr" "$file"
    mem="$_mem"
    time="$_time"
    per_event=$(fmt_per_event_kb "$mem" "$count")
    append_metric_line "$OUT" "$_run_stderr" "$file" "$file ($run_mode)"

    printf "%-10s %-8s %14s %9s %14s %10s %14s\n" \
      "$run_mode" "$size" "$mem" "$time" "$per_event" "$matching_rows" "$blob_size" | tee -a "$OUT"

    MEM_RESULTS["${run_mode}_${idx}"]="$mem"
  done
  echo "" | tee -a "$OUT"
done

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
run_memory_analyzer "$OUT" --ignore-time-anomaly-group=retention/HA-PG
