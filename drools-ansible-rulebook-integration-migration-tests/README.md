# Migration Tests

Verifies that HA persistence data created by an older runtime version can be recovered by the current version.

## How It Works

A Java CLI (`MigrationTestMain`) has two modes:

- **`--persist`** — starts an HA engine with the old runtime, sends events that create partial matches in PostgreSQL, then exits.
- **`--verify`** — starts an HA engine with the current runtime against the same database, recovers the partial matches, sends a completion event, and checks that the rule fires.

The test uses `AllCondition [a==1, b==1]`. The persist phase sends `{a:1}` events (partial matches). The verify phase sends `{b:1}` — if the recovered partials complete the rule, migration succeeded.

## Prerequisites

- Docker (for PostgreSQL)
- Python 3 (for dynamic port discovery)
- Built project: `mvn -pl drools-ansible-rulebook-integration-migration-tests -am package -DskipTests`

## Running

```bash
# Test all old versions against latest
./migration_test.sh

# Test a specific pair
./migration_test.sh 2.0.0-beta2 latest
```

## Adding a New Version

Drop the runtime HA uber-jar into `versioned-jar/<version>/`:

```bash
cp path/to/drools-ansible-rulebook-integration-runtime-<version>-HA.jar \
   versioned-jar/<version>/
```

The `latest` directory is gitignored and populated automatically from the current build.

## Structure

```
versioned-jar/
  2.0.0-beta2/   Pre-built HA uber-jar (committed)
  latest/         Current build HA uber-jar (gitignored)
lib/common.sh     Shell helpers (PG lifecycle, jar discovery)
migration_test.sh Test driver script
src/main/java/    MigrationTestMain.java
src/main/resources/
  migration_partial_match.json   Test ruleset
```
