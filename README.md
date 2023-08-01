# Drools integration with Ansible Rulebook

## Description

An integration layer allowing Ansible Rulebook to use Drools as rule engine for rules evaluation. Drools can be invoked from Ansible either via a REST API or natively through [jpy](https://pypi.org/project/jpy/).

## Manual end-to-end testing with `drools_jpy` and `ansible-rulebook`

Assuming the following local setup:

```
.
├── ansible-rulebook
├── drools-ansible-rulebook-integration
└── drools_jpy
```

For the Python projects, creating a common, shared [virtual environment](https://packaging.python.org/en/latest/glossary/#term-Virtual-Environment) instance is strongly advised, in order to be shared between `drools_jpy` and `ansible-rulebook`.
You can opt for ansible-rulebook to "borrow" the venv from drools_jpy.
Another strategy is to use a venv strategically placed (personally used `~/venv/bin/activate`).

To create the venv:

```
python3 -m venv {destination directory}
```

e.g.:

```sh
python3 -m venv ~/venv
```

Ref: https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments

Assuming the Python requirements were already installed for the projects:

- https://ansible.readthedocs.io/projects/rulebook/en/stable/development_environment.html
- https://github.com/ansible/drools_jpy#setup-and-testing 

The following steps illustrate how to perform end-to-end testing.

### Step 1 (drools-ansible-rulebook-integration)

Build locally with Maven, then copy the runtime JAR to drools_jpy:

(from `drools-ansible-rulebook-integration`)

```sh
mvn clean install
cp drools-ansible-rulebook-integration-runtime/target/drools-ansible-rulebook-integration-runtime-1.0.3-SNAPSHOT.jar ../drools_jpy/src/drools/jars/
```

### Step 2 (drools_jpy)

Use the shared venv, run the test with very-verbose and non-caputuring output, following by building (in venv site):

(from `drools_jpy`)

```sh
source ~/venv/bin/activate
pytest -vv -s
python3 -m pip install .
```

### Step 3 (ansible-rulebook)

Use the same shared venv (so to find the specific version of drools_jpy), run the tests:

(from `ansible-rulebook`)

```sh
pip install -e .
pip install -r requirements_dev.txt
pytest -m "e2e" -n auto 
pytest -m "not e2e and not long_run" -vv -n auto
```

You can always run all the tests with:

```sh
pytest
```

### Notes

An example of the strategy in practice: https://github.com/kiegroup/drools-ansible-rulebook-integration/pull/64

## Installing and Running REST API service

### Prerequisites

You will need:
  - Java 11+ installed
  - Environment variable JAVA_HOME set accordingly
  - Maven 3.8.1+ installed

When using native image compilation, you will also need:
  - [GraalVM 22.1.0](https://github.com/graalvm/graalvm-ce-builds/releases/tag/vm-22.1.0) installed
  - Environment variable GRAALVM_HOME set accordingly
  - Note that GraalVM native image compilation typically requires other packages (glibc-devel, zlib-devel and gcc) to be installed too.  You also need 'native-image' installed in GraalVM (using 'gu install native-image'). Please refer to [GraalVM installation documentation](https://www.graalvm.org/docs/reference-manual/aot-compilation/#prerequisites) for more details.

### Compile and Run in Local Dev Mode

```sh
mvn clean install
cd drools-ansible-rulebook-integration-core-rest
mvn clean compile quarkus:dev
```

### Package and Run in JVM mode

```sh
mvn clean install
cd drools-ansible-rulebook-integration-core-rest
java -jar target/quarkus-app/quarkus-run.jar
```

### Package and Run using Local Native Image
Note that this requires GRAALVM_HOME to point to a valid GraalVM installation

```sh
mvn clean install
cd drools-ansible-rulebook-integration-core-rest
mvn clean package -Pnative
```

To run the generated native executable, generated in `target/`, execute

```sh
./target/drools-ansible-rulebook-integration-core-rest-1.0.3-SNAPSHOT-runner
```

Note: This does not yet work on Windows, GraalVM and Quarkus should be rolling out support for Windows soon.

## OpenAPI (Swagger) documentation
[Specification at swagger.io](https://swagger.io/docs/specification/about/)

You can take a look at the [OpenAPI definition](http://localhost:8080/q/openapi?format=json) - automatically generated and included in this service - to determine all available operations exposed by this service. For easy readability you can visualize the OpenAPI definition file using a UI tool like for example available [Swagger UI](https://editor.swagger.io).

In addition, various clients to interact with this service can be easily generated using this OpenAPI definition.

When running in either Quarkus Development or Native mode, we also leverage the [Quarkus OpenAPI extension](https://quarkus.io/guides/openapi-swaggerui#use-swagger-ui-for-development) that exposes [Swagger UI](http://localhost:8080/q/swagger-ui/) that you can use to look at available REST endpoints and send test requests.

## Example usage

### POST /create-rules-executor

Creates a rules executor with the set of rules defined in the json payload as in the following example:

```sh
curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' -d '{
  "rules": [
    {"Rule": {
      "name": "R1",
      "condition": "sensu.data.i == 1",
      "action": {
        "assert_fact": {
          "ruleset": "Test rules4",
          "fact": {
            "j": 1
          }
        }
      }
    }},
    {"Rule": {
      "name": "R2",
      "condition": "sensu.data.i == 2",
      "action": {
        "run_playbook": [
          {
            "name": "hello_playbook.yml"
          }
        ]
      }
    }},
    {"Rule": {
      "name": "R3",
      "condition":{
        "any":[
          {
            "all":[
              "sensu.data.i == 3",
              "j == 2"
            ]
          },
          {
            "all":[
              "sensu.data.i == 4",
              "j == 3"
            ]
          }
        ]
      },
      "action": {
        "retract_fact": {
          "ruleset": "Test rules4",
          "fact": {
            "j": 3
          }
        }
      }
    }},
    {"Rule": {
      "name": "R4",
      "condition": "j == 1",
      "action": {
        "post_event": {
          "ruleset": "Test rules4",
          "fact": {
            "j": 4
          }
        }
      }
    }}
  ]
}' http://localhost:8080/create-rules-executor
```

As response it will return a simple number which is the identifier of the generated rules executor. Use this number in the URL of subsequent calls to that executor.

Note that the condition activating the rule can be a simple one, made only by one single constraint, or a nested combination of `AND` and `OR` like in `R3`. There `all` means that all conditions must be met in order to activate the rule, so it's equivalent to a `AND`, while `any` means that any of them is sufficient, equivalent to a `OR`.

### POST /rules-executors/{id}/execute

Processes the event passed in the json payload, also executing the consequences of the rules (actions) that it activates.

```
curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' -d '{ "sensu": { "data": { "i":1 } } }' http://localhost:8080/rules-executors/1/execute
```

This call, other than having the side-effect of actually executing the activated rules, returns a value representing the number of executed rules.

### POST /rules-executors/{id}/process

Processes the event passed in the json payload, without executing the consequences of the rules (actions), but only returning the list of rules activated by the event.

```
curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' -d '{ "sensu": { "data": { "i":1 } } }' http://localhost:8080/rules-executors/1/process
```

Example response:

```json
[
  {
    "ruleName":"R1",
    "facts":{
      "sensu":{
        "data":{
          "i":1
        }
      }
    }
  }
]
```

Note that if the engine is used only in this way, i.e. only to evaluate rules but not to fire them, the rules actions are useless and they can be safely omitted in the json payload defining the rule set.  
