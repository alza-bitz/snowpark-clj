---
applyTo: '**'
---

We are a Clojure wrapper for an existing Java API, so:

1. To aid those who are familiar with Snowflake and the Snowpark API, we should present the same concepts (session, dataframe, etc).
2. To aid those who are familiar with Clojure, the external API should be idiomatic Clojure.

The library should implement the following layers as namespaces:

# Core layer
- The external API used by clients.
- It should re-export functions from the other layers using import-vars from potemkin to preserve the arguments and docstrings.
- When function names in other layers have been prefixed or suffixed to avoid collisions with Clojure core, those prefixed functions should not be re-exported directly. Instead those functions should be re-exported without the prefix or suffix, whilst still preserving the arguments and docstrings
- It should use `(:refer-clojure :exclude [<functions whose names collide with clojure core>])` to avoid name collision warnings with clojure.core
- It should allow for thread last over Snowpark dataframes (as we would with lazy sequences) to build Snowpark transformations.

# Session layer
- The internal API for creating sessions, closing sessions and session management generally.
- It should provide a function giving two ways to create a session.
- The first way is by providing a map argument with keywords corresponding to the properties allowed for Snowpark, passed to SessionBuilder.configs().
- The second way is by providing the path to a properties file, passed to SessionBuilder.configFile().
- The create-session function must also provide an optional map argument that can include two options, read-key-fn which is a function to transform column names on dataset read operations, and write-key-fn which is a function to transform column names on dataset write (or create) operations.
- The default value of read-key-fn will be `(comp keyword str/lower-case)` and the default value of write-key-fn will be `(comp str/upper-case name)`, as in, read-key-fn is the inverse of write-key-fn.
- Although read-key-fn and write-key-fn are specified by the session, these functions should be passed to the convert layer functions and exclusively used there.
- The create-session function must return a map wrapping the session and the options.

# Dataframe layer
- The internal API for dataframe operations.
- Function names must be prefixed with df- when the names would otherwise collide with Clojure core.
- It should provide a function giving two ways to create a dataframe.
- The first way is without a schema arg, for development convenience. In this case the schema should be inferred from the first row.
- The second way is with a schema arg, as expected by `Session.createDataFrame(..)`
- The create-dataframe function must return a map wrapping the dataframe and the options from the session.
- Any functions that need to convert from Snowpark to Clojure or back again must use the read-key-fn or write-key-fn options from the wrapped dataset as appropriate.
- Any functions wrapping Snowpark methods that only take one or more string column name args, e.g. col(..), toDF(..), must be consistent in what will be accepted: values that will be given to write-key-fn before calling the wrapped method.
- Any functions wrapping Snowpark methods that only take one or more column object args, e.g. filter(..), sort(..), must be consistent in what will be accepted: either column objects that will be passed to the wrapped method, or values that will be given to write-key-fn and column objects created, before calling the wrapped method.
- Any functions wrapping Snowpark methods that take either string column name or column object args, e.g. select(..), groupBy(..), must be consistent in what will be accepted: either column objects that will be passed to the wrapped method, or values that will be given to write-key-fn before calling the wrapped method.
- Functions must be implemented using the Dataframe API as opposed to SQL where possible.

# Functions layer
- The internal API for Snowpark column functions and expressions.
- Function names must be suffixed with -fn when the names would otherwise collide with Clojure core.
- Use either fijit or finagle-clojure for any required Scala interop with the Snowpark Functions API, for example to support complex column expressions.

# Convert layer
- The internal API for data and schema conversions.
- It should include functions for converting data from Clojure to Snowpark and back again.
- It should include functions for converting Malli schemas to Snowpark schemas.
- All conversion functions must take read-key-fn or write-key-fn args as appropriate and use those functions when converting.
- The goal is to isolate all table column name and schema field name conversions to read-key-fn and write-key-fn functions only, with no hard-coding elsewhere.
- The read-key-fn and write-key-fn args are required, not optional, so they don't need to be defaulted.

# References to similar projects for inspiration
- Geni, a Clojure wrapper for Spark Dataframes: https://github.com/zero-one-group/geni

# All layers
- Don't hard-code the defaults for read-key-fn or write-key-fn anywhere in the code, either in part or in full. As in, no hard-coding of keyword, keyword?, name, str/upper-case, str/lower-case etc. Regarding tests, hard-coding is acceptable for unit tests, but not for integration tests.
- Don't create any namespaces with the same name.
- Don't create any code that leaves unused vars.
- Don't create any code with redundant let expressions.
- Use log4j2 for the logging implementation, and org.clojure/tools.logging for the logging api.

# All layers: all tests
- When implementing features 1 & 2, use the schema suggested by test_data.csv as the schema for all the tests.
- When implementing feature 3 onwards, create a Malli schema based on test_data.csv and use that to generate test data.
- Since we are using Malli for this solution, use Malli for all generation of test data, rather than clojure.test.check or Clojure spec.

# All layers: unit tests
- If real column objects are needed by tests, they can be created using Functions.col(..).
- We only need to mock the Snowpark API when it would otherwise need a connection to a Snowflake instance, e.g. when creating a session or calling an (eager) Snowpark action on a dataframe. In all other cases, we can just make the Snowpark API calls directly without mocking.

# All layers: integration tests
- The integration tests must verify each feature using the public API, so the core namespace should be used instead of the other internal namespaces directly.
- Every feature under test should have a single test named `test-feature-n-<description>`.
- For each feature under test, different test scenarios such as success and failure can be accommodated by using the `testing` macro from clojure.test.
- Use test fixtures to create a session and load test data to a temporary Snowflake table using dataframe/save-as-table before running each test, and to delete any temporary tables afterwards
- Use my trial Snowflake account by creating a session with the supplied snowflake.properties file.
