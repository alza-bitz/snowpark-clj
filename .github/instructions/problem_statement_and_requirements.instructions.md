---
applyTo: '**'
---

# Clojure wrapper for Snowpark

## Summary

Although the Snowpark library has Java and Scala bindings, it doesn't provide anything for Clojure. As such, it's currently not possible to interact with Snowflake using the Clojure way. It would be useful if such a wrapper existed, to enable all kinds of Snowflake use cases directly from the Clojure REPL.

## Global requirements
1. It will be a proof-of-concept, covering the essential parts of the underlying API without being too concerned with performance or completeness.
1. More specifically, in the beginning it will only support reading and writing data from local memory rather than from a Snowflake stage, and it will only support password authentication.
1. The purpose is to demonstrate the validity of the approach as a foundation for enabling a wide range of data science or engineering use cases from the Clojure REPL, in situations where Snowflake is the data warehouse of choice.
1. To aid those who are familiar with Snowflake and the Snowpark API, the same concepts should be presented with the same names (session, dataframe, etc).
1. To aid those who are familiar with Clojure, the external API should be idiomatic Clojure.

## Snowpark considerations
- Snowpark sessions are mutable and not thread-safe.
- Snowpark dataframes are immutable.
- Snowpark dataframe operations are either transformations (lazy) or actions (eager).
- Transformations defer computation until some (eager) action is called e.g. collect().
- The com.snowflake.snowpark_java package contains the main classes for the Snowpark API.
- The com.snowflake.snowpark_java.types package contains classes for defining schemas.

## Feature 1 - Load Clojure data from local and save to a Snowflake table

### Requirements
1. Create a Snowpark session
2. Create a Snowpark dataframe from a vector of maps converted to rows and a schema inferred from the first row, before wrapping Session.createDataFrame(..)
3. Write the Snowpark dataframe to a Snowflake table, wrapping DataFrameWriter.saveAsTable(..)
4. Close the session.

## Feature 2 - Compute over Snowflake table(s) on-cluster and extract a smaller result for further processing locally

### Requirements
1. Create a Snowpark session
2. Read by wrapping Session.table(..) and then compute over Snowflake table(s) on-cluster
3. Copy the result from cluster to local, wrapping DataFrame.collect()
4. Transform the result from Snowpark rows to a vector of maps
5. Close the session.

## Feature 3 - Session macros

### Requirements
- Make the session wrapper implement java.io.Closeable so that the with-open macro can be used with the result of create-session and ensure the session is closed after the body has been evaluated.

## Feature 4 - Convert Malli schemas to Snowpark schemas

### Requirements
- As per feature 1 except at step 2, the data is generated from a Malli schema and the Snowpark schema is created from the Malli schema.
- Add some generative, property-based round-trip unit tests that define a Malli schema, generate data and create a schema, then convert maps to rows, convert rows back to maps again and assert the result is equal to the generated data.
- Add some generative, property-based round-trip integration tests that define a Malli schema, generate data and create a schema, then create a dataset, collect and assert the result is equal to the generated data.

## Feature 5 - Map-like access to columns

### Requirements
The wrapped dataset should present like a Clojure map. As in, the columns of the wrapped dataset must be accessible in the same way that keys can be used to access the values of a Clojure map. I'm looking to emulate what Tablecloth does here.
```clojure
(df :name) or (:name df)
```

## Feature 6 - Convert to a tech.ml.dataset or Tablecloth dataset

### Requirements
- As per feature 2 except at step 4, transform the result from Snowpark rows, including the schema, to a tech.ml.dataset or Tablecloth dataset for further processing with Tableplot or other Scicloj libraries.

## Features that are planned but have not yet been elaborated
- Support for adding column(s) to a dataset
- Support for joins
- Streaming read: wrap DataFrame.toLocalIterator() as a reducible coll for processing with transducers
- Streaming write?
- Load data from stage using DataFrameReader
- Save data to stage using DataFrameWriter
- Authentication token expired
- Authentication other than username/password
- Create dataframe from range, wrapping Session.range()
- UDFs
- Stored procedures
- Async
- Load a tech.ml.dataset or Tablecloth dataset from local and save to a Snowflake table
