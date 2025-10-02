(ns snowpark-clj.functions
  "The internal API for Snowpark column functions and expressions."
  (:import [com.snowflake.snowpark_java Functions]))

;; Column creation functions

;; To create a column by name, see the snowpark-clj.dataset/col function

(defn lit
  "Create a column with a literal value"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "lit"
            :params ["java.lang.Object"]}
  [value]
  (Functions/lit value))

(defn expr
  "Create a column from SQL expression string"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "expr"
            :params ["java.lang.String"]}
  [expression]
  (Functions/expr expression))

(defn count-fn
  "Returns either the number of non-NULL records for the specified columns, or the total number of records."
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "count"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/count col))

;; Math functions

(defn abs-fn
  "Absolute value"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "abs"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/abs col))

(defn max-fn
  "Maximum value"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "max"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/max col))

(defn min-fn
  "Minimum value"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "min"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/min col))

(defn avg
  "Average value"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "avg"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/avg col))

(defn sum
  "Sum of values"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "sum"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/sum col))

;; String functions

(defn upper
  "Convert to uppercase"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "upper"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/upper col))

(defn lower
  "Convert to lowercase"
  #:scanner{:class "com.snowflake.snowpark_java.Functions"
            :method "lower"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col]
  (Functions/lower col))
