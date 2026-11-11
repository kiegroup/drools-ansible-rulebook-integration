# Release Procedure (1.0.x)

Release procedure for drools-ansible-rulebook-integration 1.0.x branch.

## Versioning Policy

- The pom.xml version remains **SNAPSHOT** throughout the release (e.g. `1.0.13-SNAPSHOT`)
- A **tag** is created for each release candidate (e.g. `1.0.13-beta1`, `1.0.13-beta2`, ...)
- The tag is passed to the PNC build, which performs the release build
- After the official EDA release, bump to the next SNAPSHOT version

## Branch Mapping

| Branch | Version Example | Corresponding Drools Branch |
|--------|----------------|-----------------------------|
| `1.0.x` | 1.0.13-SNAPSHOT | `9.106.x-prod`      |

## Prerequisites

- JDK 17 or later
- Maven 3.9.x
- Push access to the GitHub repository
- The corresponding Drools branch (`9.106.x-prod`) must be in a buildable state

## 1. Pre-release Preparation

### Drools Branch Update

Consider upgrading to the latest `9.X.x-prod` branch (e.g. `9.106.x-prod`) even if the current branch mapping points to an older one. Compare the branches to confirm that no ansible-specific fixes would be lost and that additional bug/memory fixes would be gained. If upgrading, update the Branch Mapping table above and follow "Update Drools Branch Mapping" below.

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
| ANTLR 4 | `version.org.antlr4` |
| ANTLR 3 runtime | `version.org.antlr` |
| Eclipse JDT Core Compiler (ECJ) | `version.org.eclipse.jdt` |
| JMH | `version.jmh` |

### Update Drools Branch Mapping (if needed)

If the dependent Drools branch has changed, update both:

- `.ci/project-dependencies.yaml`
- `.ci/jenkins/Jenkinsfile.prod.nightly`

## 2. Build and Test

```bash
# Full build
mvn clean install

# Load tests and memory leak analysis
cd drools-ansible-rulebook-integration-main
./load_test_all.sh
```

Record results in the "EDA Drools Memory consumption" spreadsheet.

Save load test output for the release record:

```bash
mkdir -p drools-ansible-rulebook-integration-main/report/<version>
cp drools-ansible-rulebook-integration-main/result_all.txt \
   drools-ansible-rulebook-integration-main/report/<version>/
```

## 3. Create Tag and Push

Keep the pom.xml at SNAPSHOT and create the tag.

```bash
git tag 1.0.13-beta1
mvn clean test
git push origin 1.0.13-beta1
git push upstream 1.0.13-beta1
```

The latest commit build is available at https://github.com/kiegroup/drools-ansible-rulebook-integration/releases/tag/1.0.x-latest (auto-built by `pre-release.yml`).

Use the `drools-ansible-rulebook-integration-runtime` JAR for drools_jpy (create a PR to drools_jpy).

## 5. PNC Build

Pass the tag to the PNC build. PNC performs the release build from the tagged source. This becomes the official EDA release.

Post-release tasks other than the non-beta tag can proceed in parallel with the PNC build.

### If PNC rejects the build

If PNC returns the build for fixes, create a patch branch from the beta tag and continue there:

```bash
git checkout -b 1.0.13.x 1.0.13-beta1
# apply fixes, then tag 1.0.13-beta2 and resubmit to PNC
```

Alternatively, skip the version entirely and advance to the next patch (e.g. 1.0.14) on the `1.0.x` branch.

## 6. Post-release

### Create non-beta tag

Create a final release tag on the same commit as the beta tag used for the release (e.g. 1.0.13-beta1). This must wait until the PNC build succeeds.

```bash
git tag 1.0.13 1.0.13-beta1
git push origin 1.0.13
git push upstream 1.0.13
```

### Version bump

After the beta tag is pushed, update to the next SNAPSHOT version. This can proceed without waiting for PNC.

Create a branch for the post-release changes and submit as a PR:

```bash
git checkout -b bump-to-1.0.14 1.0.x
```

```bash
# e.g. 1.0.13-SNAPSHOT -> 1.0.14-SNAPSHOT
find . -name "pom.xml" -exec sed -i 's/1.0.13-SNAPSHOT/1.0.14-SNAPSHOT/g' {} +
```

```bash
git add -A
git commit -m "bump to 1.0.14-SNAPSHOT"
git push origin 1.0.x
```

All 8 pom.xml files are affected:
- `pom.xml` (parent)
- `drools-ansible-rulebook-integration-api/pom.xml`
- `drools-ansible-rulebook-integration-protoextractor/pom.xml`
- `drools-ansible-rulebook-integration-runtime/pom.xml`
- `drools-ansible-rulebook-integration-main/pom.xml`
- `drools-ansible-rulebook-integration-benchmark/pom.xml`
- `drools-ansible-rulebook-integration-core-rest/pom.xml`
- `drools-ansible-rulebook-integration-tests/pom.xml`

### Forward port to main branch

Forward port changes from 1.0.x to main.
