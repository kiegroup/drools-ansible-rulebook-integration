#!/usr/bin/env bash

# Usage: ./load_test.sh [1k|10k|100k|1m]
#
# Defaults to “1k” if you don’t pass any argument. It will run two test files:
#   24kb_<size>_events.json
#   24kb_<size>_events_unmatch.json
# and append both results into result_<size>.txt

# 1) Pick up the size argument, default to "1k"
size="${1:-1k}"

# 2) Validate it
case "$size" in
  1k|10k|100k|1m) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [1k|10k|100k|1m]"
    exit 1
    ;;
esac

# 3) Prepare filenames for “match” and “unmatch”
match_file="24kb_${size}_events.json"
unmatch_file="24kb_${size}_events_unmatch.json"

# 4) Truncate (or create) a fresh result file
out="result_${size}.txt"
> "$out"

# 5) Run both tests in sequence
for file in "$match_file" "$unmatch_file"; do

  echo "Running on $file..."
  java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar \
       "$file" 2>> "$out"
done

echo "All runs complete for size=${size}. See $out for the STDERR output."