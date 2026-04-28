## Test tips

Other than usual unit tests, we have some irregular tests:

- **MemoryLeakTest**: This test checks for memory leaks in the code. As they are slow and its results are not deterministic, they are not run by default. To test them, run `mvn test -Pmemoryleak-tests`.
- **load test scripts**: The `drools-ansible-rulebook-integration-load-tests` module contains standalone HA/noHA scripts for manual and CI load testing. Build the fat jar with `mvn -pl drools-ansible-rulebook-integration-load-tests -am package -DskipTests`, then run the target script from that module directory. CI currently runs `load_test_match_unmatch_noHA_HA-PG.sh`, `load_test_retention_noHA_HA-PG.sh`, `load_test_temporal_HA-PG.sh`, and `load_test_failover_HA-PG.sh`. The other scripts `load_test_match.sh`, `load_test_unmatch.sh` and `load_test_match_unmatch_noHA.sh` are for quick testing with manual run.

The above 2 tests are relatively important to detect memory leak issues. Added to github action `pull-request.yml`.

- **PerfTest**: This test contains various and relatively high load tests, which are run by default.
- **SlownessTest**: This test verifies the behavior under the real slowness (but not very long). It is run by default.
