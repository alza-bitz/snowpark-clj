---
applyTo: '**'
---

# UAT Tests

## Feature 1 - Load data from local and save to a Snowflake table

### Load Clojure data and save to Snowflake table
- Given a Snowpark session and vector of Clojure maps
- When creating a dataframe and saving to table
- Then data is successfully persisted and retrievable

## Feature 2 - Compute over Snowflake table(s) on-cluster to produce a smaller result for local processing

### Read from table, compute, and collect results  
- Given data saved in a Snowflake table
- When applying transformations and collecting results
- Then computed results match expected filtered and sorted data

## Feature 3 - Session macros

### Session macros work correctly
- Given a session created with with-open macro
- When performing dataframe operations
- Then session is automatically closed after execution

## Feature 4 - Create Snowpark schemas from Malli schemas

### Create Snowpark schema from Malli schema
- Given a Malli schema and generated test data
- When creating dataframe with schema and collecting
- Then round-trip data integrity is preserved

## Feature 5 - Map-like access to columns

### IFn access: (df :column)
- Given a dataframe with columns
- When accessing columns using function syntax
- Then Column objects are returned for valid queries

### ILookup access: (:column df)  
- Given a dataframe with columns
- When accessing columns using keyword lookup
- Then Column objects are returned for valid queries

### Non-existent column returns nil
- Given a dataframe
- When accessing non-existent columns
- Then nil is returned for both access patterns

### Mixed column access patterns work together
- Given a dataframe
- When combining IFn and ILookup access patterns
- Then both patterns work correctly in the same query

### Column access with case transformation
- Given a dataframe with various case column names
- When accessing columns with different cases
- Then proper case transformation occurs and queries succeed

## Feature 6 - Transform to a tech.ml.dataset or Tablecloth dataset

### Tablecloth dataset creation succeeds
- Given collected Snowpark data
- When converting to Tablecloth dataset
- Then dataset is created with correct dimensions and columns

### Key transformations are preserved
- Given Snowflake uppercase column names
- When converting to Tablecloth
- Then columns are transformed to lowercase keywords

### Data values are correctly preserved  
- Given original and collected data
- When comparing values
- Then all data values survive the round trip correctly

### Tablecloth operations work correctly
- Given a Tablecloth dataset from Snowpark
- When performing Tablecloth operations
- Then operations work correctly and can be chained with Snowpark

## Additional Tests

### Various data type conversions work correctly
- Given mixed data types including integers, strings, booleans, and timestamps
- When creating and collecting dataframe
- Then all data types are correctly preserved

### Library handles larger datasets efficiently  
- Given a large dataset of 1000 records
- When creating dataframe and applying transformations
- Then operations complete successfully with correct results

