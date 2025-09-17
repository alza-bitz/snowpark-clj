# snowpark-clj

A Clojure API for [Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/java/index) and [Snowflake](https://www.snowflake.com).

[![CI/CD Status](https://github.com/alza-bitz/snowpark-clj/actions/workflows/build.yml/badge.svg)](https://github.com/alza-bitz/snowpark-clj/actions/workflows/build.yml)

## Summary

Although the Snowpark library has Java and Scala bindings, it doesn't provide anything for Clojure. As such, it's currently not possible to interact with Snowflake using the Clojure way. This library provides a proof-of-concept wrapper to enable all kinds of Snowflake use cases directly from the Clojure REPL.

To aid those who are already familiar with Snowpark, existing names are retained where possible. Nouns e.g. `session`, `dataframe` are used for namespaces, and verbs e.g. `select`, `filter`, `collect` are used for functions.

Note: Since Snowpark is based on the Snowflake JDBC driver, this wrapper inherits that model and as such it is fundamentally a row-oriented API.

## Aims

Validate the approach as a foundation for enabling a wide range of data science or data engineering use cases from the Clojure REPL, in situations where Snowflake is the data warehouse of choice.

## Features

All currently implemented features are summarised here. The [problem statement and requirements instructions](.github/instructions/problem_statement_and_requirements.instructions.md) give more details for each feature.

1. **Load data from local and save to a Snowflake table** - Create Snowpark sessions and dataframes from Clojure data structures, then save to Snowflake tables
2. **Compute over Snowflake table(s) on-cluster to produce a smaller result for local processing** - Read from Snowflake tables, perform computations on-cluster, and collect results back to Clojure
3. **Session macros** - Use `with-open` macro support for automatic session cleanup
4. **Create Snowpark schemas from Malli schemas** - As per feature 1 except the Snowpark schema is created from a Malli schema instead of being inferred from the data
5. **Map-like access to columns** - Access dataframe columns using Clojure map semantics (`(df :column)` or `(:column df)`)
6. **Transform to a tech.ml.dataset or Tablecloth dataset** - As per feature 2, but transforming Snowpark results to the [Scicloj](https://scicloj.github.io) ecosystem for further analysis

As a proof-of-concept, this library covers the essential parts of the underlying API without being too concerned about performance or completeness. More specifically, it currently only supports reading and writing data from local memory rather than Snowflake stages or streaming, and it only supports password authentication. However, these and other more advanced features are [noted and planned](.github/instructions/problem_statement_and_requirements.instructions.md#features-that-are-planned-but-have-not-yet-been-elaborated) pending further elaboration.

## Prerequisites

- Java 11 or 17
- Clojure 1.12.0 or later

Alternatively, use an editor or environment that supports [dev containers](https://containers.dev). The [basecloj](https://github.com/scicloj/devcontainer-templates/tree/main/src/basecloj) dev container template will install all the above prerequisites.

- A Snowflake account, db and schema, plus a user and role with the required permissions e.g. create tables, read/write data etc.
- A `snowflake.edn` configuration file

## Configuration
To create a session, a Snowflake configuration is required. This can be provided as an edn file or a Clojure map. Either way, it will be validated against a [config schema](https://github.com/alza-bitz/snowpark-clj/blob/main/src/snowpark_clj/config.clj) on load.

### Read password from an environment variable

Use an [#env](https://github.com/juxt/aero#env) tagged literal to read the password from an environment variable.

`snowflake.edn`
```clojure
{:url "https://your-account.snowflakecomputing.com"
 :user "your-username"
 :password #mask #env SNOWFLAKE_PASSWORD
 :role "your-role"
 :warehouse "your-warehouse"
 :db "your-database"
 :schema "your-schema"}
```

Export your password:

```bash
$ export SNOWFLAKE_PASSWORD="your-actual-password"
```

### Read password from an include file
Use an [#include](https://github.com/juxt/aero#include) tagged literal to read the password from a separate file.

`snowflake.edn`
```clojure
{:url "https://your-account.snowflakecomputing.com"
 :user "your-username"
 :password #mask #include "password.edn"
 :role "your-role"
 :warehouse "your-warehouse"
 :db "your-database"
 :schema "your-schema"}
```

`password.edn`
```clojure
"your-actual-password"
```
Note: `password.edn` needs to be in the same directory as `snowflake.edn`. If the config file is in your project, make sure to add the password file to your `.gitignore` file!

## Usage

Add the `snowpark-clj` dependency:

`deps.edn`
```clojure
{:deps {snowpark-clj/snowpark-clj {:git/sha "fd6ea012b464f4d000b31b70ebd9773e3659e019"}}}
```

Then start a REPL. This example uses the command-line tool, but you can use your favourite editor-connected REPL instead.

```bash
$ clj
```

If you are using Java 17 or later, you will also need to [provide a JVM option](https://community.snowflake.com/s/article/JDBC-Driver-Compatibility-Issue-With-JDK-16-and-Later) as follows:

`deps.edn`
```clojure
{:deps {snowpark-clj/snowpark-clj {:git/sha "fd6ea012b464f4d000b31b70ebd9773e3659e019"}}
 :aliases
 {:repl {:jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"]}}}
```

Then start a REPL with the alias:

```bash
$ clj -M:repl
```

### Feature 1: Save data to a Snowflake table

```clojure
(require '[snowpark-clj.core :as sp])

;; Sample data
(def employee-data
  [{:id 1 :name "Alice" :age 25 :department "Engineering" :salary 75000}
   {:id 2 :name "Bob" :age 30 :department "Marketing" :salary 65000}
   {:id 3 :name "Charlie" :age 35 :department "Engineering" :salary 80000}])

;; Create session and save data
(with-open [session (sp/create-session "snowflake.edn")]
  (-> (sp/create-dataframe session employee-data)
      (sp/save-as-table "employees" :overwrite)))
```

### Feature 2: Compute over a table on-cluster and extract a smaller result for local processing

```clojure
(with-open [session (sp/create-session "snowflake.edn")]
  (let [table-df (sp/table session "employees")]
    (-> table-df
        (sp/filter (sp/gt (sp/col table-df :salary) (sp/lit 70000)))
        (sp/select [:name :salary])
        (sp/collect))))
;; => [{:name "Alice" :salary 75000} {:name "Charlie" :salary 80000}]
```

### Feature 3: Session management using `with-open`

```clojure
;; Session automatically closed after block execution
(with-open [session (sp/create-session "snowflake.edn")]
  (let [df (sp/table session "employees")]
    (sp/row-count df)))
```

### Feature 4: Create Snowpark schemas from Malli schemas

```clojure
(require '[malli.core :as m]
         '[malli.generator :as mg])

;; Schema with optional age field
(def employee-schema-with-optional-keys
  (m/schema [:map
             [:id :int]
             [:name :string] 
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]
             [:age {:optional true} :int]]))

;; Generate test data from schema
(def test-data (mg/sample employee-schema-with-optional-keys {:size 10}))

(with-open [session (sp/create-session "snowflake.edn")]
  (let [schema (sp/malli->schema session employee-schema-with-optional-keys)]
    (-> (sp/create-dataframe session test-data schema)  ; Explicit schema conversion
        (sp/save-as-table "employees_generated" :overwrite))))
```

### Feature 5: Map-like column access

```clojure
(with-open [session (sp/create-session "snowflake.edn")]
  (let [df (sp/table session "employees")]
    ;; Both forms work - get Column objects for use in queries
    (let [name-col (df :name)           ; IFn access: (df :column)
          salary-col (:salary df)]      ; ILookup access: (:column df)
      (-> df
          (sp/select [name-col salary-col])
          (sp/filter (sp/gt salary-col (sp/lit 70000)))
          (sp/collect)))))
```

### Feature 6: Convert to a tech.ml.dataset or Tablecloth dataset

```clojure
(require '[tablecloth.api :as tc])

(with-open [session (sp/create-session "snowflake.edn")]
  (let [table-df (sp/table session "employees")
        collected-data (-> table-df
                           (sp/filter (sp/gt (sp/col table-df :salary) (sp/lit 60000)))
                           (sp/collect))
        dataset (tc/dataset collected-data)]
    ;; Now use with Scicloj ecosystem
    (tc/info dataset)))
```

## Development

### Prerequisites

- Github CLI and Docker (If you want to run the GitHub Actions workflow locally)

Alternatively, use an editor or environment that supports [dev containers](https://containers.dev). The supplied [devcontainer.json](https://github.com/alza-bitz/nrepl-ws-server/blob/main/.devcontainer/devcontainer.json) will install all the above prerequisites.

### Approach
The [development approach instructions](.github/instructions/development_approach.instructions.md) provide details on the development approach and methodology used.

### Running Tests

```bash
# Unit tests
$ clojure -M:test

# Integration tests (requires Snowflake connection)
export SNOWFLAKE_PASSWORD="your-actual-password"
$ clojure -M:integration

# UAT tests (requires Snowflake connection)
export SNOWFLAKE_PASSWORD="your-actual-password"
$ clojure -M:uat

# All tests
export SNOWFLAKE_PASSWORD="your-actual-password"
$ clojure -M:test && clojure -M:integration && clojure -M:uat
```

### Running GitHub Actions Locally

Add `SNOWFLAKE_PASSWORD=your-actual-password` to `./actsecrets`

```bash
$ gh auth login
$ gh extension install https://github.com/nektos/gh-act
$ gh act
```

### REPL Development

Start a REPL with development dependencies:

```bash
$ clj -M:dev
```

The library uses structured logging with SLF4J. Configure logging levels in `resources/simplelogger.properties`.

## Acknowledgements

This library is built on top of the [Snowpark Java API](https://docs.snowflake.com/en/developer-guide/snowpark/java/index.html).

Inspired by:
- [Geni](https://github.com/zero-one-group/geni) - Clojure wrapper for Spark DataFrames
- [Tablecloth](https://github.com/scicloj/tablecloth) - In-memory dataset manipulation

## License

Copyright © 2025 Alex Coyle

Distributed under the [Eclipse Public License](LICENSE) either version 2.0 or (at your option) any later version.
