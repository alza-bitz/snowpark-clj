---
applyTo: '**'
---

# All tests
- For any test fixtures, use-fixtures from clojure.test should be used.
- For interactive development, use the REPL rather than the command line for checking that test namespaces load and checking the tests pass.
- For property-based tests at unit or integration test scope, use `defspec` from `clojure.test.check`.
- Property-based tests must be named `<property-based-test-name>-property`.
- For spot checks, you can run the tests for a specific namespace using `clojure -M:<alias> -n <test-ns>`

# Unit tests
- Unit tests must be placed in the `test` directory, with `-test` appended to the namespace of the functions under test.
- Every public function should have a single test named `test-<function-name>`.
- For each function under test, different test scenarios such as success, failure or edge cases can be accommodated by using the `testing` macro from clojure.test.
- In general, test data should be generated rather than fixed, using whatever libary is specified in the solution design instructions.
- In general, property-based tests should be preferred to example-based tests.
- Any side effecting function calls in the function under test must be mocked, so use with-redefs or reify with `spy.core` and `spy.assert` from `tortue/spy` to assert the correct numbers of calls and expected args.
- Any mocking must be limited to only where it is needed, such as mocking specific function calls inside the function under test, rather than mocking the whole function under test.
- Don't leak any knowledge of mocks into the code under test.
- If you need to mock any macros, use `macroexpand` beforehand to inspect the returned expansion and see what functions need to be mocked.
- For spot checks, you can run the unit tests on the command line with `clojure -M:test`

# Integration tests
- Integration tests must be placed in the `integration` directory, with `-integration-test` appended to the namespace of the code under test.
- Every public function should have a single test named `test-<function-name>`.
- For each function under test, different test scenarios such as success and failure can be accommodated by using the `testing` macro from clojure.test.
- In general, the integration tests for the functions in any given namespace should follow the same structure as the unit tests for that same namespace, but without any mocking.
- For spot checks, you can run the integration tests on the command line with `clojure -M:integration`

# UAT tests
- UAT tests must be placed in the `uat` directory, with `-uat-test` appended to the namespace of the code under test.
