#!/usr/bin/env bash

# Usage: ./load_test_all.sh
# This script runs a series of test files. While you can confirm STDOUT, the results are redirected to result_all.txt through STDERR.
# The result would be "testfile name, usedMemory (bytes), timeTaken (ms)"
# -------- for example --------
# 24kb_1k_events.json, 5246824, 192
# 24kb_10k_events.json, 5221216, 648
# 24kb_100k_events.json, 5285568, 3958
# 24kb_1m_events.json, 5282712, 33741
# 24kb_1k_events_unmatch.json, 5158648, 122
# 24kb_10k_events_unmatch.json, 5166952, 364
# 24kb_100k_events_unmatch.json, 5249480, 1682
# 24kb_1m_events_unmatch.json, 5291136, 12940

# Truncate (or create) result_all.txt so we start fresh each time
> result_all.txt

# List of JSON files to process
files=(
  "24kb_1k_events.json"
  "24kb_10k_events.json"
  "24kb_100k_events.json"
  "24kb_1m_events.json"
  "24kb_1k_events_unmatch.json"
  "24kb_10k_events_unmatch.json"
  "24kb_100k_events_unmatch.json"
  "24kb_1m_events_unmatch.json"
)

# Loop over each file and run the java command
for file in "${files[@]}"; do
  echo "Running on ${file}..."
  java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar \
       "${file}" 2>> result_all.txt
done

echo "All runs complete. See result_all.txt for any STDERR output (testfile, usedMemory, timeTaken)."
