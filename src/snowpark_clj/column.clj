(ns snowpark-clj.column)

;; Functions to compare columns (Column instance methods)

(defn gt
  "Greater than comparison"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "gt"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.gt col1 col2))

(defn lt
  "Less than comparison"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "lt"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.lt col1 col2))

(defn eq
  "Equal comparison"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "equal_to"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.equal_to col1 col2))

(defn geq
  "Greater than or equal comparison"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "geq"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.geq col1 col2))

(defn leq
  "Less than or equal comparison"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "leq"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.leq col1 col2))

(defn neq
  "Not equal comparison"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "not_equal"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.not_equal col1 col2))

;; Functions for logical column operations (also Column instance methods)

(defn and-fn
  "Logical AND"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "and"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.and col1 col2))

(defn or-fn
  "Logical OR"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "or"
            :params ["com.snowflake.snowpark_java.Column"]}
  [col1 col2]
  (.or col1 col2))

(defn not-fn
  "Logical NOT"
  #:scanner{:class "com.snowflake.snowpark_java.Column"
            :method "not"
            :params []}
  [col]
  (.not col))

(defn parse-quoted
  "Parses the column name according to whether it is quoted or not."
  [col-name]
  (when col-name 
    (->> col-name
       (re-matches #"\"(.*)\"|(.*)")
       rest
       (zipmap [:quoted :unquoted])
       (merge {:col-name col-name
               :unquoted ""}))))

(defn quoted
  "Transforms the column name to avoid issues with parentheses in keywords.
   
   Aggregated column names are themselves quoted, for example '\"COUNT(<COLNAME>)\"', '\"AVG(<COLNAME>)\"', etc.
   These will be unquoted and transformed to 'COUNT-<COLNAME>', 'AVG-<COLNAME>'.
   
   Any other quoted column names are not supported and an exception will be thrown."
  [col-name]
  (when-let [{:keys [quoted unquoted]} (parse-quoted col-name)]
    (or
     unquoted
     (if-let [matches (re-matches #"(\w+)\((.+)\)" quoted)]
       (str (nth matches 1) "-" (nth matches 2))
       (throw (ex-info (str "Quoted column names are not supported: " col-name) {:col-name col-name}))))))