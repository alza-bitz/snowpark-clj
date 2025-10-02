---
applyTo: '**'
---

# All docs
- **All code must be tested in the REPL before it is added to the documentation!**

# README.md
- CI/CD badges: unit tests, integration tests, uat tests
- Summary
- Aims
- Features: give a summary and link to the full list of features in my instruction files.
- Prerequisites: installed tools, Snowflake account, snowflake.edn, SNOWFLAKE_PASSWORD, etc.
- Usage: concise code examples demonstrating each of the numbered features
- Development: how to run the tests
- Acknowledgements: link to the Snowpark Java API
- License: copyright and distribution terms

# docs/api-coverage.md
- A report on the coverage of Snowpark API calls, what is currently supported and what is not.
- By "supported" I mean that there is a function in the external API wrapping the corresponding Snowpark Java API method.
- The source of truth is the Snowpark Java API dependency as defined in deps.edn.
- The report will essentially be a "left outer join" of the Snowpark Java API and the Clojure wrapper functions in the core namespace.
- More specifically, a left outer join of class, method, and parameter types.
- We do a left outer join because I want to emphasize what is not yet supported in the Clojure wrapper.
- It will include summary statistics:
  - Count of Snowpark Java API classes
  - Count of Snowpark Java API methods (count of left join)
  - Count of supported classes
  - Count of supported methods (count of inner join)
- It will include two tables with the following columns, the first table for the inner join (supported methods) and the second table for the left outer join (all methods):
  - Snowpark Java API class w/ link to the Javadoc for the class
  - Snowpark Java API method
  - Supported (yes/no)
  - Clojure namespace qualified function w/ link to the source code of the namespace
- The table will be generated from the codebase, so it can be updated automatically as the code changes.
