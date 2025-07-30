## Test tips

Other than usual unit tests, we have some irregular tests:

- **MemoryLeakTest**: This test checks for memory leaks in the code. As they are slow and its results are not deterministic, they are not run by default. To test them, run `mvn test -Pmemoryleak-tests`.
- **load_test_all.sh**: This script runs a load test with different numbers of events, to check memory usage tendencies. It is not run by maven. You can run it manually with `./load_test_all.sh`. `load_test.sh` is a flexible version of that. See the comments in the script for more information.

The above 2 tests are relatively important to detect memory leak issues. Make sure to run them from time to time, especially before releasing a new version.

- **PerfTest**: This test contains various and relatively high load tests, which are run by default.
- **SlownessTest**: This test verifies the behavior under the real slowness (but not very long). It is run by default.