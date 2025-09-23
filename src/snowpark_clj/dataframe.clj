(ns snowpark-clj.dataframe
  "The internal API for Snowpark dataframe functions."
  (:require
   [snowpark-clj.convert :as convert]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.wrapper :as wrapper]))

;; Helper functions for consistent column argument handling

(defn- normalize-to-vector
  "Normalize single value or vector to vector"
  [args]
  (if (sequential? args) args [args]))

(defn- to-column-names
  "Category 1: For Snowpark methods that only take string column names.
   Accepts column keys that will be transformed using key->col-fn, returns string array."
  [cols key->col-fn]
  (let [normalized-cols (normalize-to-vector cols)]
    (into-array String (map key->col-fn normalized-cols))))

(defn- to-columns
  "Category 2: For Snowpark methods that only take Column objects.
   Accepts Column objects or column keys that will be transformed using key->col-fn, returns Column array."
  [cols key->col-fn raw-df]
  (let [normalized-cols (normalize-to-vector cols)
        column-objects (map (fn [col]
                              (if (instance? com.snowflake.snowpark_java.Column col)
                                col
                                (.col raw-df (key->col-fn col))))
                            normalized-cols)]
    (into-array com.snowflake.snowpark_java.Column column-objects)))

(defn- to-columns-or-names
  "Category 3: For Snowpark methods that take either strings or Columns.
   Returns appropriate array (String[] or Column[]) based on whether cols are Column objects or column keys that will be transformed using key->col-fn."
  [cols key->col-fn]
  (let [normalized-cols (normalize-to-vector cols)
        first-col (first normalized-cols)]
    (if (instance? com.snowflake.snowpark_java.Column first-col)
      (into-array com.snowflake.snowpark_java.Column normalized-cols)
      (into-array String (map key->col-fn normalized-cols)))))

;; Dataframe creation functions

(defn
  ^#:scanner{:class "com.snowflake.snowpark_java.Session"
             :method "createDataFrame"
             :params ["com.snowflake.snowpark_java.Row[]" "com.snowflake.snowpark_java.types.StructType"]}
  create-dataframe
  "Create a dataframe from a collection of maps.
   
   Two-arity version:
   Args:
   - session: Session wrapper
   - data: Collection of maps representing rows
   
   Three-arity version (Feature 4):
   Args:
   - session: Session wrapper
   - data: Collection of maps representing rows
   - schema: Snowpark StructType schema for explicit schema definition
   
   Returns: A dataframe wrapper with the session options and map-like column access"
  ([session data]
   (when (empty? data)
     (throw (IllegalArgumentException. "Cannot create dataframe from empty data"))) 
   (create-dataframe session data (schema/infer-schema session data))) 
  ([session data schema]
   (when (empty? data)
     (throw (IllegalArgumentException. "Cannot create dataframe from empty data"))) 
   (let [raw-session (wrapper/unwrap session)
         key->col-fn (wrapper/unwrap-option session :key->col-fn)
         rows (convert/maps->rows data schema key->col-fn)
         dataframe (.createDataFrame raw-session rows schema)]
     (wrapper/wrap-dataframe dataframe (wrapper/unwrap-options session)))))

(defn table
  "Create a dataframe from a Snowflake table.
   
   Args:
   - session: Session wrapper
   - table-name: Name of the table
   
   Returns: A dataframe wrapper with the session options and map-like column access"
  [session table-name]
  (let [raw-session (wrapper/unwrap session)
        dataframe (.table raw-session table-name)]
    (wrapper/wrap-dataframe dataframe (wrapper/unwrap-options session))))

(defn sql
  "Execute SQL query and return a wrapped dataframe.
   
   Args:
   - session: Session wrapper
   - query: SQL query to execute
   
   Returns: A dataframe wrapper with the session options and map-like column access"
  [session query]
  (let [raw-session (wrapper/unwrap session)
        dataframe (.sql raw-session query)]
    (wrapper/wrap-dataframe dataframe (wrapper/unwrap-options session))))

;; Dataframe transformation functions (lazy)

(defn 
  ^#:scanner{:class "com.snowflake.snowpark_java.DataFrame"
             :method "select"
             :params [["java.lang.String[]"]
                      ["com.snowflake.snowpark_java.Column[]"]]}
  select
  "Select columns from a dataframe.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or column keys that will be transformed using key->col-fn)
   
   Returns: A dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        key->col-fn (wrapper/unwrap-option df :key->col-fn)
        col-array (to-columns-or-names cols key->col-fn)
        result-df (.select raw-df col-array)]
    (wrapper/wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn
  ^#:scanner{:class "com.snowflake.snowpark_java.DataFrame"
             :method "filter"
             :params ["com.snowflake.snowpark_java.Column"]}
  df-filter
  "Filter rows in a dataframe using a condition.
   
   Args:
   - df: Dataframe wrapper
   - condition: Column object or column key that will be transformed using key->col-fn
   
   Returns: A dataframe wrapper"
  [df condition]
  (let [raw-df (wrapper/unwrap df)
        key->col-fn (wrapper/unwrap-option df :key->col-fn)
        col-condition (if (instance? com.snowflake.snowpark_java.Column condition)
                        condition
                        (.col raw-df (key->col-fn condition)))
        result-df (.filter raw-df col-condition)]
    (wrapper/wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn where
  "Alias for df-filter"
  [df condition]
  (df-filter df condition))

(defn limit
  "Limit the number of rows in a dataframe.
   
   Args:
   - df: Dataframe wrapper
   - n: Number of rows to limit to
   
   Returns: A dataframe wrapper"
  [df n]
  (let [raw-df (wrapper/unwrap df)
        result-df (.limit raw-df n)]
    (wrapper/wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn df-sort
  "Sort a dataframe by columns.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or column keys that will be transformed using key->col-fn)
   
   Returns: A dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        key->col-fn (wrapper/unwrap-option df :key->col-fn)
        col-array (to-columns cols key->col-fn raw-df)
        result-df (.sort raw-df col-array)]
    (wrapper/wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn df-group-by
  "Group a dataframe by columns.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or column keys that will be transformed using key->col-fn)
   
   Returns: A grouped dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        key->col-fn (wrapper/unwrap-option df :key->col-fn)
        col-array (to-columns-or-names cols key->col-fn)
        result-grouped-df (.groupBy raw-df col-array)]
    (wrapper/wrap-grouped result-grouped-df (wrapper/unwrap-options df))))

(defn agg
  "Computes an aggregation according to the supplied columns.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or column keys that will be transformed using key->col-fn)

   Returns: a dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        key->col-fn (wrapper/unwrap-option df :key->col-fn)
        col-array (to-columns-or-names cols key->col-fn)
        result-df (.agg raw-df col-array)]
    (wrapper/wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn join
  "Join two dataframes.
   
   Args:
   - left: Dataframe wrapper
   - right: Dataframe wrapper
   - join-exprs: Join condition(s)
   - join-type: Optional join type (:inner, :left, :right, :outer)
   
   Returns: A dataframe wrapper"
  ([left right join-exprs]
   (join left right join-exprs :inner))
  ([left right join-exprs join-type]
   (let [left-raw (wrapper/unwrap left)
         right-raw (wrapper/unwrap right)
         join-type-str (case join-type
                         :inner "inner"
                         :left "left"
                         :right "right"
                         :outer "outer"
                         :full "outer"
                         (name join-type))
         result (.join left-raw right-raw join-exprs join-type-str)]
     (wrapper/wrap-dataframe result (wrapper/unwrap-options left)))))

;; Dataframe action functions (eager)

(defn collect
  "Collect all rows from a dataframe to local memory.
   
   Args:
   - df: Dataframe wrapper
   
   Returns: Vector of maps with column names transformed using col->key-fn"
  [df]
  (let [raw-df (wrapper/unwrap df)
        col->key-fn (wrapper/unwrap-option df :col->key-fn)
        rows-array (.collect raw-df)
        schema (.schema raw-df)]
    (convert/rows->maps rows-array schema col->key-fn)))

(defn show
  "Display dataframe contents (limited number of rows).
   
   Args:
   - df: Dataframe wrapper
   - n: Optional number of rows to show (default 20)
   
   Side effect: Prints to stdout"
  ([df] (show df 20))
  ([df n]
   (let [raw-df (wrapper/unwrap df)]
     (.show raw-df n))))

(defn row-count
  "Count the number of rows in a dataframe.
   
   Args:
   - df: Dataframe wrapper
   
   Returns: Long"
  [df]
  (let [raw-df (wrapper/unwrap df)]
    (.count raw-df)))

(defn df-take
  "Take the first n rows from a dataframe.
   
   Args:
   - df: Dataframe wrapper
   - n: Number of rows to take
   
   Returns: Vector of maps with column names transformed using col->key-fn"
  [df n]
  (let [limited-df (limit df n)]
    (collect limited-df)))

;; Dataframe write operations

(defn save-as-table
  "Save a dataframe as a table in Snowflake.
   
   Args:
   - df: Dataframe wrapper
   - table-name: Name of the table to create
   - mode: Write mode (:overwrite, :append, :error-if-exists, :ignore) - optional
   - options: Optional map of Snowflake-specific options:
     {:partition-by [\"col1\" \"col2\"]     ; Partition columns
      :cluster-by [\"col1\" \"col2\"]       ; Cluster columns
      :table-type \"transient\"             ; Table type
      ...}                                  ; Other Snowflake writer options"
  ([df table-name]
   (let [raw-df (wrapper/unwrap df)
         writer (.write raw-df)]
     (.saveAsTable writer table-name)))
  ([df table-name mode]
   (save-as-table df table-name mode {}))
  ([df table-name mode options]
   (let [raw-df (wrapper/unwrap df)
         writer (.write raw-df)
         mode-str (case mode
                    :overwrite "overwrite"
                    :append "append"
                    :error-if-exists "errorifexists"
                    :ignore "ignore"
                    (name mode))
         ;; Set mode first
         writer (.mode writer mode-str)
         ;; Set additional options if provided
         writer (if (seq options)
                  (.options writer (into {} (map (fn [[k v]] [(name k) v]) options)))
                  writer)]
     (.saveAsTable writer table-name))))

;; Column reference functions

(defn col
  "Get a column reference from a dataframe.
   
   Args:
   - df: Dataframe wrapper
   - col-name: column key (will be transformed using key->col-fn)
   
   Returns: Snowpark Column object"
  [df col-name]
  (let [raw-df (wrapper/unwrap df)
        key->col-fn (wrapper/unwrap-option df :key->col-fn)]
    (.col raw-df (key->col-fn col-name))))

;; Utility functions

(defn schema
  "Get the schema of a dataframe"
  [df]
  (let [raw-df (wrapper/unwrap df)]
    (.schema raw-df)))
