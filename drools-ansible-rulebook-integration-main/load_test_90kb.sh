#!/usr/bin/env bash

# Usage: ./load_test_90kb.sh [1k|10k|100k|1m]
#
# Defaults to "1k" if you don't pass any argument. It will run two test files:
#   90kb_<size>_events.json
#   90kb_<size>_events_unmatch.json
# and append both results into result_90kb_<size>.txt

size="${1:-1k}"

case "$size" in
  1k|10k|100k|1m) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [1k|10k|100k|1m]"
    exit 1
    ;;
esac

match_file="90kb_${size}_events.json"
unmatch_file="90kb_${size}_events_unmatch.json"

out="result_90kb_${size}.txt"
> "$out"

#for file in "$match_file" "$unmatch_file"; do
for file in "$match_file"; do
  echo "Running on $file..."
  java -Xmx200m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar \
       "$file" 2>> "$out"
done

echo "All runs complete for size=${size}. See $out for the STDERR output."
