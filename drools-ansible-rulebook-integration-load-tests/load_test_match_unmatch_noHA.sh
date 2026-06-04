#!/usr/bin/env bash
# Usage: ./load_test_match_unmatch_noHA.sh
#
# Loops 4 sizes x {match, unmatch} x {noHA} = 8 runs.
# noHA-only: no Docker/Postgres required.
# Emits one combined result_match_unmatch_noHA.txt and runs MemoryLeakAnalyzer.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

SIZES=("1k" "10k" "100k" "1m")
SCENARIOS=("match" "unmatch")

OUT="result_match_unmatch_noHA.txt"
LOG="out_match_unmatch_noHA.log"
> "$OUT"
> "$LOG"

require_jar

for size in "${SIZES[@]}"; do
  for scenario in "${SCENARIOS[@]}"; do
    if [ "$scenario" = "match" ]; then
      file="24kb_${size}_events.json"
    else
      file="24kb_${size}_events_unmatch.json"
    fi
    label="$file (noHA)"
    echo "Running $label..."
    jvm_run "$label" "$file"
    echo "$_run_stderr" | grep "^${file}" | tail -1 >> "$OUT" || echo "$file (noHA), FAILED, FAILED" >> "$OUT"
  done
done

echo ""
echo "All 8 runs complete. Result lines:"
cat "$OUT"
echo ""

echo "Running MemoryLeakAnalyzer..."
java -cp "target/classes:target/drools-ansible-rulebook-integration-load-tests-jar-with-dependencies.jar" \
     org.drools.ansible.rulebook.integration.loadtests.analyze.MemoryLeakAnalyzer "$OUT"
ANALYZER_EXIT=$?
echo "Analyzer exit code: $ANALYZER_EXIT"
exit $ANALYZER_EXIT
