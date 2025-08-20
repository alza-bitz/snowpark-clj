(ns snowpark-clj.functions
  "Column functions and expressions for Snowpark"
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

;; Comparison functions (these are Column instance methods)

(defn gt
  "Greater than comparison"
  [col1 col2]
  (.gt col1 col2))

(defn lt
  "Less than comparison" 
  [col1 col2]
  (.lt col1 col2))

(defn eq
  "Equal comparison"
  [col1 col2]
  (.equal_to col1 col2))

(defn gte
  "Greater than or equal comparison"
  [col1 col2]
  (.geq col1 col2))

(defn lte
  "Less than or equal comparison"
  [col1 col2]
  (.leq col1 col2))

(defn neq
  "Not equal comparison"
  [col1 col2]
  (.not_equal col1 col2))

;; Logical functions (also Column instance methods)

(defn and-fn
  "Logical AND"
  [col1 col2]
  (.and col1 col2))

(defn or-fn
  "Logical OR"
  [col1 col2]
  (.or col1 col2))

(defn not-fn
  "Logical NOT"
  [col]
  (.not col))

;; Math functions (these might be static on Functions)

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
