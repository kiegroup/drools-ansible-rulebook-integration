# Load Tests

Measures memory usage and elapsed time for the rule engine under various event loads, comparing noHA vs HA-PostgreSQL modes. A `MemoryLeakAnalyzer` checks whether memory scales linearly with event count — super-linear growth signals a leak.

## How It Works

A Java CLI (`LoadTestMain`) reads a pre-generated event JSON file, feeds events into the rule engine, and reports peak memory (bytes) and elapsed time (ms) on stderr. Shell scripts orchestrate multiple runs across sizes and modes, collect results into `result_*.txt`, and optionally run `MemoryLeakAnalyzer`.

## Prerequisites

- Docker (for PostgreSQL, required by HA-PG scripts)
- Python 3 (for dynamic port discovery)
- Fat JAR: `mvn -pl drools-ansible-rulebook-integration-load-tests -am package -DskipTests`

## Running

### All batch tests at once

```bash
cd drools-ansible-rulebook-integration-load-tests
./run_all_load_tests.sh
```

This runs all 5 CI batch scripts in sequence and builds the fat JAR automatically if missing.

### Individual batch scripts

| Script | What it tests | Docker |
|--------|--------------|--------|
| `load_test_match_unmatch_noHA.sh` | 4 sizes x match/unmatch, noHA only (8 runs) | No |
| `load_test_match_unmatch_noHA_HA-PG.sh` | 3 sizes x match/unmatch x noHA/HA-PG (12 runs) | Yes |
| `load_test_retention_noHA_HA-PG.sh` | Memory growth from partial-match event accumulation (100/500/1k events) | Yes |
| `load_test_temporal_HA-PG.sh` | `once_within` temporal rule under rapid ingress (100/500/1k events, HA-PG only) | Yes |
| `load_test_failover_HA-PG.sh` | HA failover recovery time — load then recover (100/500/1k events, HA-PG only) | Yes |

### Ad-hoc single-size scripts

```bash
./load_test_match.sh [1k|10k|100k|1m]
./load_test_unmatch.sh [1k|10k|100k|1m]
```

These run one size in both noHA and HA-PG modes. Require Docker.

## Output

- `result_*.txt` — tab-formatted metric summaries
- `out_*.log` — full JVM stdout/stderr logs

## Build Tips

The fat JAR (`*-jar-with-dependencies.jar`) is built by `maven-assembly-plugin` and bundles all transitive dependencies — including the HA modules (`ha-core`, `ha-postgres`). These come via the `drools-ansible-rulebook-integration-runtime` dependency, which pulls in the HA PostgreSQL implementation through its own POM.

If you modify HA code (e.g. `PostgreSQLStateManager`), you must rebuild with `-am` to pick up the change:

```bash
mvn -pl drools-ansible-rulebook-integration-load-tests -am clean package -DskipTests
```

Without `-am`, the assembly uses the previously installed artifacts from `~/.m2` and your changes won't be included. Without `clean`, the assembly plugin may reuse the old JAR.

## Structure

```
lib/common.sh              Shell helpers (PG lifecycle, JVM runner, metric parsing)
run_all_load_tests.sh      Runs all 5 batch scripts
load_test_*.sh             Individual test scripts
src/main/java/
  LoadTestMain.java        CLI entry point
  LoadRunner.java          noHA runner
  HaLoadRunner.java        HA-PG runner
  HaFailoverRecoveryRunner.java  Failover recovery runner
  Measurement.java         Memory/time capture
  MetricReporter.java      Stderr metric output
  OutcomeCheck.java        Expected-outcome validation
  gen/PayloadGenerator.java  Event JSON generator
  analyze/MemoryLeakAnalyzer.java  Linear-regression leak detector
src/main/resources/        Pre-generated event JSON files
```
