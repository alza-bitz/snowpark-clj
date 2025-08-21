(ns snowpark-clj.functions
  "The internal API for Snowpark column functions and expressions."
  (:import [com.snowflake.snowpark_java Functions]))

;; Column creation functions

;; To create a column by name, see the snowpark-clj.dataset/col function

(defn lit
  "Create a column with a literal value"
  [value]
  (Functions/lit value))

(defn expr
  "Create a column from SQL expression string"
  [expression]
  (Functions/expr expression))

(defn count-fn
  "Returns either the number of non-NULL records for the specified columns, or the total number of records."
  [col]
  (Functions/count col))

;; Math functions

(defn abs-fn
  "Absolute value"
  [col]
  (Functions/abs col))

(defn max-fn
  "Maximum value"
  [col]
  (Functions/max col))

(defn min-fn
  "Minimum value"
  [col]
  (Functions/min col))

(defn avg
  "Average value"
  [col]
  (Functions/avg col))

(defn sum
  "Sum of values"
  [col]
  (Functions/sum col))

;; String functions

(defn upper
  "Convert to uppercase"
  [col]
  (Functions/upper col))

(defn lower
  "Convert to lowercase"
  [col]
  (Functions/lower col))
