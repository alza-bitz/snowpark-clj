# snowpark-clj

A Clojure API for [Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/java/index) and [Snowflake](https://www.snowflake.com).

[![CI/CD Status](https://github.com/alza-bitz/snowpark-clj/actions/workflows/build.yml/badge.svg)](https://github.com/alza-bitz/snowpark-clj/actions/workflows/build.yml)

## Summary

Although the Snowpark library has Java and Scala bindings, it doesn't provide anything for Clojure. As such, it's currently not possible to interact with Snowflake using the Clojure way. This library provides a proof-of-concept wrapper to enable all kinds of Snowflake use cases directly from the Clojure REPL.

## Aims

To aid those who are familiar with Snowflake and the Snowpark API, the same concepts are presented with the same names (session, dataframe, etc). To aid those who are familiar with Clojure, the external API tries to present as idiomatic Clojure.

The aim is to validate this approach as a foundation for enabling a wide range of data science or data engineering use cases from the Clojure REPL, in situations where Snowflake is the data warehouse of choice.

## Features

As a proof-of-concept, it covers the essential parts of the underlying API without being too concerned with performance or completeness. More specifically, it currently only supports reading and writing data from local memory rather than from Snowflake stages or streaming, and it only supports password authentication. These and other more advanced features are noted and [planned](#planned-features), pending further elaboration.

1. **Load Clojure data from local and save to a Snowflake table** - Create Snowpark sessions and dataframes from Clojure data structures, then save to Snowflake tables
2. **Compute over Snowflake table(s) on-cluster and extract results locally** - Read from Snowflake tables, perform computations on-cluster, and collect results back to Clojure
3. **Session macros** - Use `with-open` macro support for automatic session cleanup
4. **Convert Malli schemas to Snowpark schemas** - Generate Snowpark schemas from Malli specifications with property-based testing
5. **Map-like access to columns** - Access dataframe columns using Clojure map semantics (`(df :column)` or `(:column df)`)
6. **Convert to a tech.ml.dataset or Tablecloth dataset** - Transform Snowpark results to Scicloj ecosystem datasets for further analysis

### Planned Features

- Support for adding column(s) to a dataset
- Support for joins
- Streaming read and write operations
- Authentication beyond username/password
- UDFs and stored procedures
- Async operations
- Load from/save to Snowflake stages

## Prerequisites

- Java 11 or 17
- Clojure 1.12.0 or later
- If you want to run the GitHub Actions workflow locally: Github CLI and Docker

Alternatively, use an editor or environment that supports dev containers. The supplied [devcontainer.json](https://github.com/alza-bitz/nrepl-ws-server/blob/main/.devcontainer/devcontainer.json) will install all the above prerequisites.

- A Snowflake account, db and schema, plus a user and role with the required permissions e.g. create tables, read/write data etc.
- A `snowflake.edn` configuration file
- A `SNOWFLAKE_PASSWORD` environment variable

### Snowflake Configuration

Create a `snowflake.edn` file in your project root:

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
export SNOWFLAKE_PASSWORD="your-actual-password"
```

## Usage

### Feature 1: Load Clojure data and save to Snowflake

```clojure
(require '[snowpark-clj.core :as sp])

;; Sample data
(def employee-data
  [{:id 1 :name "Alice" :age 25 :department "Engineering" :salary 75000}
   {:id 2 :name "Bob" :age 30 :department "Marketing" :salary 65000}
   {:id 3 :name "Charlie" :age 35 :department "Engineering" :salary 80000}])

;; Create session and save data
(with-open [session (sp/create-session "snowflake.edn")]
  (-> employee-data
      (sp/create-dataframe session)
      (sp/save-as-table "employees" :overwrite)))
```

### Feature 2: Read from Snowflake and process locally

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
    (sp/count df)))
```

### Feature 4: Using Malli schemas

#### Example 1: Schema inference from data

```clojure
(require '[malli.core :as m]
         '[malli.generator :as mg])

(def employee-schema
  [:map
   [:id [:int {:min 1}]]
   [:name :string]
   [:age [:int {:min 18 :max 65}]]
   [:department [:enum "Engineering" "Marketing" "Sales"]]
   [:salary [:int {:min 30000 :max 200000}]]])

;; Generate test data from schema - schema inferred from first row
(def test-data (mg/sample employee-schema 100))

(with-open [session (sp/create-session "snowflake.edn")]
  (-> test-data
      (sp/create-dataframe session)  ; Schema inferred automatically
      (sp/save-as-table "test_employees" :overwrite)))
```

#### Example 2: Create Snowpark schema from Malli schema

```clojure
;; Schema with optional age field
(def employee-schema-with-optional-keys
  [:map
   [:id :int]
   [:name :string] 
   [:department [:enum "Engineering" "Marketing" "Sales"]]
   [:salary [:int {:min 50000 :max 100000}]]
   [:age {:optional true} :int]])

;; Generate test data from schema
(def test-data (mg/sample employee-schema-with-optional-keys 100))

(with-open [session (sp/create-session "snowflake.edn")]
  (let [schema (sp/malli->schema session employee-schema-with-optional-keys)]
    (-> test-data
        (sp/create-dataframe session schema)  ; Explicit schema conversion
        (sp/save-as-table "test_employees_with_optional_age" :overwrite))))
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

### Running Tests

```bash
# Unit tests
clojure -M:test

# Integration tests (requires Snowflake connection)
export SNOWFLAKE_PASSWORD="your-actual-password"
clojure -M:integration

# UAT tests (requires Snowflake connection)
export SNOWFLAKE_PASSWORD="your-actual-password"
clojure -M:uat

# All tests
export SNOWFLAKE_PASSWORD="your-actual-password"
clojure -M:test && clojure -M:integration && clojure -M:uat
```

### Running GitHub Actions Locally

Add `SNOWFLAKE_PASSWORD=your-actual-password` to `./actsecrets`

```bash
gh extension install https://github.com/nektos/gh-act
gh act
```

### REPL Development

Start a REPL with development dependencies:

```bash
clojure -M:dev
```

The library uses structured logging with SLF4J. Configure logging levels in `resources/simplelogger.properties`.

### Project Structure

- `src/snowpark_clj/` - Main library code organized by layer (core, session, dataframe, etc.)
- `test/` - Unit tests
- `integration/` - Integration tests requiring Snowflake connection
- `uat/` - User acceptance tests for each feature
- `dev/` - Development utilities

## Acknowledgements

This library is built on top of the [Snowpark Java API](https://docs.snowflake.com/en/developer-guide/snowpark/java/index.html).

Inspired by:
- [Geni](https://github.com/zero-one-group/geni) - Clojure wrapper for Spark DataFrames
- [Tablecloth](https://github.com/scicloj/tablecloth) - In-memory dataset manipulation

## License

Copyright © 2025 Alex Coyle

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
