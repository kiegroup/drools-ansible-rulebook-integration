# Release Procedure

Release procedure for drools-ansible-rulebook-integration.

## Versioning Policy

- The pom.xml version remains **SNAPSHOT** throughout the release (e.g. `2.0.3-SNAPSHOT`)
- A **tag** is created for each release candidate (e.g. `2.0.3-beta1`, `2.0.3-beta2`, ...)
- The tag is passed to the PNC build, which performs the release build
- After the official EDA release, bump to the next SNAPSHOT version

## Branch Mapping

| Branch | Version Example | Corresponding Drools Branch |
|--------|----------------|----------------------------|
| `2.0.x` | 2.0.3-SNAPSHOT | `9.106.x-prod` |
| `1.0.x` | 1.0.12-SNAPSHOT | `9.103.x-prod-ansible` |

## Prerequisites

- JDK 17 or later
- Maven 3.9.x
- Push access to the GitHub repository
- The corresponding Drools branch must be in a buildable state

## 1. Pre-release Preparation

### Dependency Updates

Confirm that all dependencies are at their latest versions before release. If a dependency is intentionally kept at an older version, document the reason as a comment in the pom.xml next to the property. Review and update the following:

| Dependency | Property Name |
|-----------|--------------|
| Drools | `version.drools` |
| Jackson (stay on 2.21.x LTS) | `version.com.fasterxml.jackson` |
| Apache Commons Lang | `version.org.apache.commons.lang3` |
| Apache Commons Text | `version.org.apache.commons.text` |
| Apache Commons Codec | `version.commons-codec` |
| SnakeYAML | `version.org.yaml.snakeyaml` |
| SLF4J | `version.org.slf4j` |
| BouncyCastle | `version.bouncycastle` |
| PostgreSQL JDBC | `version.postgresql` |
| H2 | `version.h2` |
| HikariCP | `version.hikaricp` |
| ANTLR 4 | `version.org.antlr4` |
| ANTLR 3 runtime | `version.org.antlr` |
| Eclipse JDT Core Compiler (ECJ) | `version.org.eclipse.jdt` |
| TestContainers | `version.testcontainers` |

### Update Drools Branch Mapping (if needed)

If the dependent Drools branch has changed, update both:

- `.ci/project-dependencies.yaml` (main and 2.0.x)
- `Jenkinsfile.prod.nightly` (2.0.x)

(e.g. 694ff870, 8301fee2, 93778d80)

## 2. Build and Test

```bash
# Full build
mvn clean install

# HA tests (PostgreSQL)
cd drools-ansible-rulebook-integration-ha
mvn verify -Dtest.db.type=postgres

# Memory leak tests
cd drools-ansible-rulebook-integration-api
mvn -Pmemoryleak-tests test

# Migration tests
# Before running, verify that versioned-jar/ contains JARs for all existing released versions
cd drools-ansible-rulebook-integration-migration-tests
./migration_test.sh

# Load tests
cd drools-ansible-rulebook-integration-load-tests
./load_test_match_unmatch_noHA_HA-PG.sh
./load_test_retention_noHA_HA-PG.sh
./load_test_temporal_HA-PG.sh
./load_test_failover_HA-PG.sh
```

Record results from load_test_match_unmatch_noHA.sh and load_test_match_unmatch_HA-PG.sh in the "EDA Drools Memory consumption" spreadsheet.

Save all load test outputs for the release record:

```bash
mkdir -p drools-ansible-rulebook-integration-load-tests/report/<version>
cp drools-ansible-rulebook-integration-load-tests/out_*.log \
   drools-ansible-rulebook-integration-load-tests/result_*.txt \
   drools-ansible-rulebook-integration-load-tests/report/<version>/
```

## 3. Create Tag and Push

Keep the pom.xml at SNAPSHOT and create the tag.

```bash
git tag 2.0.3-beta1
mvn clean test
git push origin 2.0.3-beta1
git push upstream 2.0.3-beta1
```

The latest commit build is available at https://github.com/kiegroup/drools-ansible-rulebook-integration/releases/tag/2.0.x-latest (auto-built by `pre-release.yml`).

Use the `drools-ansible-rulebook-integration-runtime` HA uber-jar for drools_jpy (create a PR to drools_jpy).

## 5. PNC Build

Pass the tag to the PNC build. PNC performs the release build from the tagged source. This becomes the official EDA release.

## 6. Post-release

### Create non-beta tag

Create a final release tag on the same commit as the beta tag used for the release (e.g. 2.0.3-beta1).

```bash
git tag 2.0.3
git push origin 2.0.3
git push upstream 2.0.3
```

### Version bump

After ansible-rulebook is officially released, update to the next SNAPSHOT version and `DROOLS_VERSION`.

```bash
# e.g. 2.0.3-SNAPSHOT -> 2.0.4-SNAPSHOT
find . -name "pom.xml" -exec sed -i 's/2.0.3-SNAPSHOT/2.0.4-SNAPSHOT/g' {} +
```

Also update `AbstractHAStateManager.DROOLS_VERSION` to match the new version:

```
drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-core/
  src/main/java/org/drools/ansible/rulebook/integration/ha/api/AbstractHAStateManager.java
```

```java
public static final String DROOLS_VERSION = "2.0.4";
```

```bash
git add -A
git commit -m "bump to 2.0.4-SNAPSHOT"
git push origin 2.0.x
```

All 15 pom.xml files are affected:
- `pom.xml` (parent)
- `drools-ansible-rulebook-integration-api/pom.xml`
- `drools-ansible-rulebook-integration-protoextractor/pom.xml`
- `drools-ansible-rulebook-integration-runtime/pom.xml`
- `drools-ansible-rulebook-integration-main/pom.xml`
- `drools-ansible-rulebook-integration-benchmark/pom.xml`
- `drools-ansible-rulebook-integration-tests/pom.xml`
- `drools-ansible-rulebook-integration-load-tests/pom.xml`
- `drools-ansible-rulebook-integration-migration-tests/pom.xml`
- `drools-ansible-rulebook-integration-core-rest/pom.xml`
- `drools-ansible-rulebook-integration-ha/pom.xml`
- `drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-core/pom.xml`
- `drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-h2/pom.xml`
- `drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-postgres/pom.xml`
- `drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests/pom.xml`

### Update migration test JARs

After the official release, update the JARs used for migration tests.

Place the new version's uber-jar in:

```
drools-ansible-rulebook-integration-migration-tests/lib/
```

and update the test scripts accordingly.

### Forward port to main branch

Forward port changes from 2.0.x to main.
