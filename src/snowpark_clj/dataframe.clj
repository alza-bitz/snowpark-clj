(ns snowpark-clj.dataframe
  "The internal API for Snowpark dataframe functions."
  (:require
   [snowpark-clj.convert :as convert]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.wrapper :as wrapper]))

(defn- wrap-dataframe
  "Wrap a Snowpark DataFrame with session options and map-like column access"
  [df opts]
  (let [base-map (merge {:dataframe df} opts)]
    
    ;; Return a map that implements IFn and ILookup for map-like column access
    (reify
      clojure.lang.ILookup
      (valAt [this k]
        (.valAt this k nil))
      (valAt [_ k not-found]
        (let [write-key-fn (:write-key-fn base-map)
              decoded-name (write-key-fn k)
              raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (set (.names schema))]
          (if (contains? field-names decoded-name)
            (.col raw-df decoded-name)
            not-found)))
      
      clojure.lang.IFn
      (invoke [this k]
        (.valAt this k))
      (invoke [this k not-found]
        (.valAt this k not-found))
      
      clojure.lang.IPersistentMap
      (assoc [_ _ _]
        (throw (UnsupportedOperationException. "Cannot assoc on dataframe wrapper")))
      (without [_ _]
        (throw (UnsupportedOperationException. "Cannot dissoc on dataframe wrapper")))
      (iterator [this]
        ;; Support keys and vals operations
        (.iterator (.seq this)))
      
      clojure.lang.Associative
      (containsKey [_ k]
        (let [write-key-fn (:write-key-fn base-map)
              decoded-name (write-key-fn k)
              raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (set (.names schema))]
          (contains? field-names decoded-name)))
      (entryAt [this k]
        (when (.containsKey this k)
          (clojure.lang.MapEntry. k (.valAt this k))))
      
      clojure.lang.IPersistentCollection
      (count [_]
        ;; Return the number of fields in the DataFrame schema
        (let [raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (.names schema)]
          (alength field-names)))
      (empty [_]
        {})
      (equiv [_ o]
        (and (map? o) (= base-map o)))
      
      clojure.lang.Seqable
      (seq [_]
        ;; Return a seq of MapEntry objects, each containing the encoded field name and column object
        (let [raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (.names schema)
              read-key-fn (:read-key-fn base-map)]
          (map (fn [field-name]
                 (let [encoded-name (read-key-fn field-name)
                       column-obj (.col raw-df field-name)]
                   (clojure.lang.MapEntry. encoded-name column-obj)))
               field-names)))
      
      ;; Additional map-like operations for DataFrame columns
      clojure.lang.IKVReduce
      (kvreduce [this f init]
        (reduce (fn [acc [k v]] (f acc k v)) init (.seq this)))
      
      clojure.lang.IHashEq
      (hasheq [_]
        (.hasheq base-map))
      
      Object
      (hashCode [_]
        (.hashCode base-map))
      (equals [this o]
        (.equiv this o))
      (toString [_]
        (.toString base-map))
      
      ;; Implement the protocol for accessing wrapped dataframe
      wrapper/IWrappedSessionOptions
      (unwrap [_] (:dataframe base-map))
      (unwrap-option [_ option-key] (get base-map option-key))
      (unwrap-options [_] (dissoc base-map :dataframe)))
      ))

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
        decoded-cols (map write-key-fn normalized-cols)]
    (into-array String decoded-cols)))

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

;; Dataframe creation functions

(defn create-dataframe
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
         write-key-fn (wrapper/unwrap-option session :write-key-fn)
         rows (convert/maps->rows data schema write-key-fn)
         dataframe (.createDataFrame raw-session rows schema)]
     (wrap-dataframe dataframe (wrapper/unwrap-options session)))))

(defn table
  "Create a dataframe from a Snowflake table.
   
   Args:
   - session: Session wrapper
   - table-name: Name of the table
   
   Returns: A dataframe wrapper with the session options and map-like column access"
  [session table-name]
  (let [raw-session (wrapper/unwrap session)
        dataframe (.table raw-session table-name)]
    (wrap-dataframe dataframe (wrapper/unwrap-options session))))

(defn sql
  "Execute SQL query and return a wrapped dataframe.
   
   Args:
   - session: Session wrapper
   - query: SQL query to execute
   
   Returns: A dataframe wrapper with the session options and map-like column access"
  [session query]
  (let [raw-session (wrapper/unwrap session)
        dataframe (.sql raw-session query)]
    (wrap-dataframe dataframe (wrapper/unwrap-options session))))

;; Dataframe transformation functions (lazy)

(defn select
  "Select columns from a dataframe.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or encoded column names that will be decoded using write-key-fn)
   
   Returns: A dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        write-key-fn (wrapper/unwrap-option df :write-key-fn)
        col-array (to-columns-or-names cols write-key-fn)
        result-df (.select raw-df col-array)]
    (wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn df-filter
  "Filter rows in a dataframe using a condition.
   
   Args:
   - df: Dataframe wrapper
   - condition: Column object or encoded column name that will be decoded using write-key-fn
   
   Returns: A dataframe wrapper"
  [df condition]
  (let [raw-df (wrapper/unwrap df)
        write-key-fn (wrapper/unwrap-option df :write-key-fn)
        col-condition (if (instance? com.snowflake.snowpark_java.Column condition)
                        condition
                        (.col raw-df (write-key-fn condition)))
        result-df (.filter raw-df col-condition)]
    (wrap-dataframe result-df (wrapper/unwrap-options df))))

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
    (wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn df-sort
  "Sort a dataframe by columns.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or encoded column names that will be decoded using write-key-fn)
   
   Returns: A dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        write-key-fn (wrapper/unwrap-option df :write-key-fn)
        col-array (to-columns cols write-key-fn raw-df)
        result-df (.sort raw-df col-array)]
    (wrap-dataframe result-df (wrapper/unwrap-options df))))

(defn df-group-by
  "Group a dataframe by columns.
   
   Args:
   - df: Dataframe wrapper
   - cols: Single column or vector of columns (Column objects or encoded column names that will be decoded using write-key-fn)
   
   Returns: A grouped dataframe wrapper"
  [df cols]
  (let [raw-df (wrapper/unwrap df)
        write-key-fn (wrapper/unwrap-option df :write-key-fn)
        col-array (to-columns-or-names cols write-key-fn)
        result-grouped-df (.groupBy raw-df col-array)]
    (wrap-dataframe result-grouped-df (wrapper/unwrap-options df))))

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
     (wrap-dataframe result (wrapper/unwrap-options left)))))

;; Dataframe action functions (eager)

(defn collect
  "Collect all rows from a dataframe to local memory.
   
   Args:
   - df: Dataframe wrapper
   
   Returns: Vector of maps with keys encoded using read-key-fn"
  [df]
  (let [raw-df (wrapper/unwrap df)
        read-key-fn (wrapper/unwrap-option df :read-key-fn)
        collected-rows (.collect raw-df)
        schema (.schema raw-df)]
    (convert/rows->maps collected-rows schema read-key-fn)))

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

(defn df-count
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
   
   Returns: Vector of maps with keys encoded using read-key-fn"
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
   - col-name: encoded column name (will be decoded using write-key-fn)
   
   Returns: Snowpark Column object"
  [df col-name]
  (let [raw-df (wrapper/unwrap df)
        write-key-fn (wrapper/unwrap-option df :write-key-fn)
        decoded-name (write-key-fn col-name)]
    (.col raw-df decoded-name)))

;; Utility functions

(defn schema
  "Get the schema of a dataframe"
  [df]
  (let [raw-df (wrapper/unwrap df)]
    (.schema raw-df)))
