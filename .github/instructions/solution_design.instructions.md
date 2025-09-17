---
applyTo: '**'
---

The library should implement the following namespaces:

# Core
- The external API used by clients.
- It should be based on the facade pattern, where we expose the required functions in a uniform manner.
- It should re-export functions from the other namespaces using import-vars from potemkin to preserve the arguments and docstrings.
- When function names in other namespaces have been prefixed or suffixed to avoid collisions with Clojure core, those prefixed functions should not be re-exported directly. Instead those functions should be re-exported without the prefix or suffix, whilst still preserving the arguments and docstrings
- It should use `(:refer-clojure :exclude [<functions whose names collide with clojure core>])` to avoid name collision warnings with clojure.core
- It should allow for thread first over Snowpark dataframes (as we would with Tablecloth datasets) to build transformations.

# Session
- The internal API for Snowpark session functions.
- It should be based on the builder pattern, where we wrap the Snowpark session in a map and modify state through successive function calls.
- It should provide a create-session function giving two ways to create a session, depending on the type of the mandatory config argument.
- If the config arg is a string, it is assumed to be the path of an edn file that is expected to conform to a Malli config schema when loaded. The Aero library should be used to load the config, validated with Malli and then passed to SessionBuilder.configs().
- Otherwise the config arg is assumed to be a map that is expected to conform to a Malli config schema. The config should be validated with Malli and then passed to SessionBuilder.configs().
- The create-session function must also provide an optional map argument that can include two options, col->key-fn which is a function to transform column names on dataset read operations, and key->col-fn which is a function to transform column names on dataset write (or create) operations.
- The default value of col->key-fn will be `(comp keyword str/lower-case)` and the default value of key->col-fn will be `(comp str/upper-case name)`, as in, col->key-fn is the inverse of key->col-fn.
- Although col->key-fn and key->col-fn are specified by the session, these functions should be passed to the convert functions and exclusively used there.
- The create-session function must return a map wrapping the session and the options.

# Config
- The internal API for the config schema and related functions.
- The Malli config schema must correspond to the following JDBC connection parameters for Snowpark, converted to lower case and keywordized: url, user and password (required); role, warehouse, db, schema and insecureMode (optional).
- The config schema must ensure the password is masked.
- It should provide a function to read the config using the Aero library with the mask.aero namespace already loaded, to ensure any #mask tags are parsed correctly.
- Any resulting config must mask the password when printed or logged, but unmask the password when configuring the Snowpark session.

# Schema
- The internal API for Snowpark schema functions.
- It should include a function for inferring a Snowpark schema from a coll of Clojure maps and a wrapped session providing key->col-fn.
- When inferring a schema, map keys must result in a StructField with nullable=true, regardless of the value.
- It should include a function for creating a Snowpark schema from a Malli schema and a wrapped session providing key->col-fn.
- When creating a schema from a Malli schema, required keys must result in a StructField with nullable=false, and optional keys must result in a StructField with nullable=true.
- All schema field name conversions must be done with col->key-fn and key->col-fn functions only.

# Dataframe
- The internal API for Snowpark dataframe functions.
- It should be based on the builder pattern, where we wrap the Snowpark dataframe in a map and modify state through successive function calls.
- It should provide a create-dataframe function giving two ways to create a dataframe.
- The first way is without a schema arg, for development convenience. In this case the schema should be inferred from the first row.
- The second way is with a schema arg, as expected by `Session.createDataFrame(..)`
- The create-dataframe function must return a map wrapping the dataframe and the options from the session.
- Any functions that need to convert from Snowpark to Clojure or back again must use the col->key-fn or key->col-fn options from the wrapped dataset as appropriate.
- Any functions wrapping Snowpark methods that only take one or more string column name args, e.g. col(..), toDF(..), must be consistent in what will be accepted: values that will be given to key->col-fn before calling the wrapped method.
- Any functions wrapping Snowpark methods that only take one or more column object args, e.g. filter(..), sort(..), must be consistent in what will be accepted: either column objects that will be passed to the wrapped method, or values that will be given to key->col-fn and column objects created, before calling the wrapped method.
- Any functions wrapping Snowpark methods that take either string column name or column object args, e.g. select(..), groupBy(..), must be consistent in what will be accepted: either column objects that will be passed to the wrapped method, or values that will be given to key->col-fn before calling the wrapped method.
- All functions must be implemented without using string SQL expressions where possible.
- Any functions wrapping Snowpark dataframe creation methods must return a map wrapping the dataframe and the options from the session wrapper.
- Any functions wrapping Snowpark eager transformation methods must return a map wrapping the dataframe and the options from the dataframe wrapper.
- Function names must be prefixed with df- when the names would otherwise collide with Clojure core.

# Functions
- The internal API for Snowpark column functions and expressions.
- Function names must be suffixed with -fn when the names would otherwise collide with Clojure core.
- Use either fijit or finagle-clojure for any required Scala interop with the Snowpark Functions API, for example to support complex column expressions.

# Convert
- The internal API for data conversion functions.
- It should include functions for converting data from Clojure maps to Snowpark rows and back again.
- All conversion functions must take col->key-fn or key->col-fn args as appropriate and use those functions when converting.
- All table column name conversions must be done with col->key-fn and key->col-fn functions only.
- The col->key-fn and key->col-fn args are required, not optional, so they don't need to be defaulted.
- When converting from maps to rows, it must handle nullable fields by including a nil value for the column index.
- When converting from rows to maps, it must handle nullable fields by excluding the key for the column index.

# References to similar projects for inspiration
- Geni, a Clojure wrapper for Spark Dataframes: https://github.com/zero-one-group/geni

# All namespaces
- Any existing code examples in the project documentation **must** be tested at the REPL whenever you make changes to the code!
- Namespace docstrings should match the description of each namespace in the instruction files.
- The README must be updated whenever the code changes, especially where it would affect the usage examples.
- Don't hard-code the defaults for col->key-fn or key->col-fn anywhere in the code, either in part or in full. As in, no hard-coding of keyword, keyword?, name, str/upper-case, str/lower-case etc. Regarding tests, hard-coding is acceptable for unit tests, but not for integration tests.
- The names of any vars that are passed as args to functions should match the name of the args, if possible.
- Don't create any namespaces with the same name.
- Don't create any code that leaves unused vars.
- Don't create any code with redundant let expressions.
- Use the slf4j simplelogger for the logging implementation, since this is already used by the Snowpark Java API.
- Use org.clojure/tools.logging for the logging api.

# All namespaces: all tests
- When implementing features 1 & 2, use the schema suggested by test_data.csv as the schema for all the tests.
- When implementing feature 3 onwards, create a Malli schema based on test_data.csv and use that to generate test data.
- When implementing dataframe tests that compose multiple transformation operations, prefer threading macros where possible.
- Since we are using Malli for this solution, use Malli for all generation of test data, rather than clojure.test.check.

# All namespaces: unit tests
- We only need to mock the Snowpark API when it would otherwise need a connection to a Snowflake instance, e.g. when creating a session or calling an (eager) Snowpark action on a dataframe. In all other cases, we can just make the Snowpark API calls directly without mocking.
- Since the functions to create sessions and dataframes return those raw objects wrapped, any tests for functions that take wrapped sessions or dataframes and need to call methods on the raw objects can create mock objects with implementations for those specific methods and wrap the mocks before calling the function under test.
- If real column objects are required by the function under test, they can be created using Functions.col(..).

# All namespaces: integration tests
- Use test fixtures to create a session before running each test and ensure any saved tables are deleted afterwards.
- Use my trial Snowflake account by creating a session with a snowflake.edn file.
- To run the these tests using the command line, the Snowflake password will need to be exported using `source ./export-secrets.sh`.

# All namespaces: uat tests
- Every feature from the requirements instructions should have a single test named `test-feature-n-<description>`.
- These tests must use the external API, as in the core namespace and not the other internal namespaces directly.
- Use test fixtures, my trial Snowflake account and export secrets script as per the instructions for integration tests.

# All namespaces: performance tests, profiling and benchmarking
- TODO