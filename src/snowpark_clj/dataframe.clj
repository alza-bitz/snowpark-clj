(ns snowpark-clj.dataframe
  "DataFrame operations and transformations"
  (:require [snowpark-clj.convert :as convert]
            [snowpark-clj.session :as session])
  (:import [com.snowflake.snowpark_java Functions]))

;; Helper functions for wrapped DataFrames

(defn unwrap-dataframe
  "Extract the raw Snowpark DataFrame from a wrapped DataFrame"
  [df-wrapper]
  (:dataframe df-wrapper))

(defn wrap-dataframe
  "Wrap a raw Snowpark DataFrame with session options"
  [dataframe session-or-df-wrapper]
  (let [opts (select-keys session-or-df-wrapper [:read-key-fn :write-key-fn])]
    (merge {:dataframe dataframe} opts)))

;; Helper functions for consistent column argument handling

(defn- normalize-to-vector
  "Normalize single value or vector to vector"
  [args]
  (if (sequential? args) args [args]))

(defn- to-column-names
  "Category 1: For Snowpark methods that only take string column names.
   Accepts encoded column names that will be decoded using write-key-fn, returns string array."
  [cols write-key-fn]
  (let [normalized-cols (normalize-to-vector cols)
        transformed-cols (map write-key-fn normalized-cols)]
    (into-array String transformed-cols)))

(defn- to-columns
  "Category 2: For Snowpark methods that only take Column objects.
   Accepts Column objects or encoded column names that will be decoded using write-key-fn, returns Column array."
  [cols write-key-fn raw-df]
  (let [normalized-cols (normalize-to-vector cols)
        column-objects (map (fn [col]
                              (if (instance? com.snowflake.snowpark_java.Column col)
                                col
                                (.col raw-df (write-key-fn col))))
                            normalized-cols)]
    (into-array com.snowflake.snowpark_java.Column column-objects)))

(defn- to-columns-or-names
  "Category 3: For Snowpark methods that take either strings or Columns.
   Returns appropriate array (String[] or Column[]) based on whether cols are encoded column names that will be decoded using write-key-fn or Column objects."
  [cols write-key-fn]
  (let [normalized-cols (normalize-to-vector cols)
        first-col (first normalized-cols)]
    (if (instance? com.snowflake.snowpark_java.Column first-col)
      (into-array com.snowflake.snowpark_java.Column normalized-cols)
      (into-array String (map write-key-fn normalized-cols)))))

;; Core DataFrame creation functions

(defn create-dataframe
  "Create a DataFrame from Clojure data.
   
   Two-arity version:
   Args:
   - session: Snowpark session wrapper
   - data: Vector of maps representing rows
   
   Three-arity version (Feature 4):
   Args:
   - session: Snowpark session wrapper
   - data: Vector of maps representing rows
   - schema: Snowpark Schema object for explicit schema definition
   
   Returns: Map wrapping the DataFrame and options from session"
  ([session data]
   (when (empty? data)
     (throw (IllegalArgumentException. "Cannot create DataFrame from empty data"))) 
   (create-dataframe session data (convert/infer-schema data (session/get-write-key-fn session)))) 
  ([session data schema]
   (when (empty? data)
     (throw (IllegalArgumentException. "Cannot create DataFrame from empty data"))) 
   (let [raw-session (session/unwrap-session session)
         write-key-fn (session/get-write-key-fn session)
         rows (convert/maps->rows data schema write-key-fn)
         dataframe (.createDataFrame raw-session rows schema)]
    ;;  FIXME why are we not using wrap-dataframe here?
     (merge {:dataframe dataframe} 
            (select-keys session [:read-key-fn :write-key-fn])))))

(defn table
  "Create a DataFrame from a Snowflake table.
   
   Args:
   - session: Snowpark session wrapper
   - table-name: Name of the table
   
   Returns: Map wrapping the DataFrame and options from session"
  [session table-name]
  (let [raw-session (session/unwrap-session session)
        dataframe (.table raw-session table-name)]
    (wrap-dataframe dataframe session)))

(defn sql
  "Execute SQL query and return a DataFrame.
   
   Args:
   - session-wrapper: Session wrapper map
   - query-string: SQL query to execute
   
   Returns: DataFrame wrapper"
  [session-wrapper query-string]
  (let [raw-session (session/unwrap-session session-wrapper)
        dataframe (.sql raw-session query-string)]
    (merge {:dataframe dataframe}
           (select-keys session-wrapper [:read-key-fn :write-key-fn]))))

;; DataFrame transformation functions (lazy)

(defn select
  "Select columns from a DataFrame.
   
   Args:
   - df: Wrapped DataFrame
   - cols: Single column or vector of columns (Column objects or encoded column names that will be decoded using write-key-fn)
   
   Returns: Wrapped DataFrame"
  [df cols]
  (let [raw-df (unwrap-dataframe df)
        write-key-fn (:write-key-fn df)
        col-array (to-columns-or-names cols write-key-fn)
        result-df (.select raw-df col-array)]
    (wrap-dataframe result-df df)))

(defn df-filter
  "Filter rows in a DataFrame using a condition.
   
   Args:
   - df: Wrapped DataFrame
   - condition: Column object, value to transform into Column, or SQL expression string
   
   Returns: Wrapped DataFrame"
  [df condition]
  (let [raw-df (unwrap-dataframe df)
        write-key-fn (:write-key-fn df)
        col-condition (cond
                        (string? condition) (Functions/expr condition)
                        (instance? com.snowflake.snowpark_java.Column condition) condition
                        :else (.col raw-df (write-key-fn condition)))
        result-df (.filter raw-df col-condition)]
    (wrap-dataframe result-df df)))

(defn where
  "Alias for df-filter"
  [df condition]
  (df-filter df condition))

(defn limit
  "Limit the number of rows in a DataFrame.
   
   Args:
   - df: Wrapped DataFrame
   - n: Number of rows to limit to
   
   Returns: Wrapped DataFrame"
  [df n]
  (let [raw-df (unwrap-dataframe df)
        result-df (.limit raw-df n)]
    (wrap-dataframe result-df df)))

(defn df-sort
  "Sort DataFrame by columns.
   
   Args:
   - df: Wrapped DataFrame
   - cols: Single column or vector of columns (Column objects or encoded column names that will be decoded using write-key-fn)
   
   Returns: Wrapped DataFrame"
  [df cols]
  (let [raw-df (unwrap-dataframe df)
        write-key-fn (:write-key-fn df)
        col-array (to-columns cols write-key-fn raw-df)
        result-df (.sort raw-df col-array)]
    (wrap-dataframe result-df df)))

(defn df-group-by
  "Group DataFrame by columns.
   
   Args:
   - df: Wrapped DataFrame
   - cols: Single column or vector of columns (Column objects or encoded column names that will be decoded using write-key-fn)
   
   Returns: GroupedDataFrame (note: this returns raw GroupedDataFrame as it's not a DataFrame)"
  [df cols]
  (let [raw-df (unwrap-dataframe df)
        write-key-fn (:write-key-fn df)
        col-array (to-columns-or-names cols write-key-fn)]
    (.groupBy raw-df col-array)))

(defn join
  "Join two DataFrames.
   
   Args:
   - left: Wrapped DataFrame
   - right: Wrapped DataFrame  
   - join-exprs: Join condition(s)
   - join-type: Optional join type (:inner, :left, :right, :outer)
   
   Returns: Wrapped DataFrame"
  ([left right join-exprs]
   (join left right join-exprs :inner))
  ([left right join-exprs join-type]
   (let [left-raw (unwrap-dataframe left)
         right-raw (unwrap-dataframe right)
         join-type-str (case join-type
                         :inner "inner"
                         :left "left"
                         :right "right"
                         :outer "outer"
                         :full "outer"
                         (name join-type))
         result (.join left-raw right-raw join-exprs join-type-str)]
     (wrap-dataframe result left))))

;; DataFrame action functions (eager)

(defn collect
  "Collect all rows from a DataFrame to local memory.
   
   Args:
   - df: Wrapped DataFrame
   
   Returns: Vector of maps with keys encoded using read-key-fn"
  [df]
  (let [raw-df (unwrap-dataframe df)
        read-key-fn (:read-key-fn df)
        collected-rows (.collect raw-df)
        schema (.schema raw-df)]
    (convert/rows->maps collected-rows schema read-key-fn)))

(defn show
  "Display DataFrame contents (limited number of rows).
   
   Args:
   - df: Wrapped DataFrame
   - n: Optional number of rows to show (default 20)
   
   Side effect: Prints to stdout"
  ([df] (show df 20))
  ([df n]
   (let [raw-df (unwrap-dataframe df)]
     (.show raw-df n))))

(defn df-count
  "Count the number of rows in a DataFrame.
   
   Args:
   - df: Wrapped DataFrame
   
   Returns: Long"
  [df]
  (let [raw-df (unwrap-dataframe df)]
    (.count raw-df)))

(defn df-take
  "Take the first n rows from a DataFrame.
   
   Args:
   - df: Wrapped DataFrame  
   - n: Number of rows to take
   
   Returns: Vector of maps with keys encoded using read-key-fn"
  [df n]
  (let [limited-df (limit df n)]
    (collect limited-df)))

(defn first-row
  "Get the first row from a DataFrame.
   
   Args:
   - df: Wrapped DataFrame
   
   Returns: Map or nil if empty with keys encoded using read-key-fn"
  [df]
  (first (df-take df 1)))

;; DataFrame write operations

(defn write
  "Get DataFrameWriter for writing DataFrame to various destinations"
  [df]
  (let [raw-df (unwrap-dataframe df)]
    (.write raw-df)))

(defn save-as-table
  "Save DataFrame as a table in Snowflake.
   
   Args:
   - df: Wrapped DataFrame
   - table-name: Name of the table to create
   - mode: Write mode (:overwrite, :append, :error-if-exists, :ignore) - optional
   - options: Optional map of Snowflake-specific options:
     {:partition-by [\"col1\" \"col2\"]     ; Partition columns
      :cluster-by [\"col1\" \"col2\"]       ; Cluster columns
      :table-type \"transient\"            ; Table type
      ...}                               ; Other Snowflake writer options
   
   Returns: Wrapped DataFrame"
  ([df table-name]
   (let [writer (write df)
         result (.saveAsTable writer table-name)]
     (wrap-dataframe result df)))
  ([df table-name mode]
   (save-as-table df table-name mode {}))
  ([df table-name mode options]
   (let [writer (write df)
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
                  writer)
         result (.saveAsTable writer table-name)]
     (wrap-dataframe result df))))

;; Column reference functions

(defn col
  "Get a column reference from a DataFrame.
   
   Args:
   - df: Wrapped DataFrame
   - col-name: encoded column name (will be decoded using write-key-fn)
   
   Returns: Snowpark Column object"
  [df col-name]
  (let [raw-df (unwrap-dataframe df)
        write-key-fn (:write-key-fn df)
        transformed-name (write-key-fn col-name)]
    (.col raw-df transformed-name)))

;; Utility functions

(defn schema
  "Get the schema of a DataFrame"
  [df]
  (let [raw-df (unwrap-dataframe df)]
    (.schema raw-df)))
