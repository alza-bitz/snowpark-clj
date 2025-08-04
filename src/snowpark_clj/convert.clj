(ns snowpark-clj.convert
  "Data conversion utilities between Clojure and Snowpark"
  (:require [malli.core :as m] 
            [clojure.string :as str])
  (:import [com.snowflake.snowpark_java Row]
           [com.snowflake.snowpark_java.types StructType StructField DataTypes]
           [java.sql Date Timestamp]))

;; Type mapping from Clojure values to Snowpark DataTypes
(defn ^:private clojure-value->data-type
  "Infer Snowpark DataType from a Clojure value"
  [value]
  (cond
    (integer? value) DataTypes/IntegerType
    (string? value) DataTypes/StringType
    (float? value) DataTypes/DoubleType
    (decimal? value) (DataTypes/createDecimalType 38 18) ; Default precision and scale
    (boolean? value) DataTypes/BooleanType
    (instance? Date value) DataTypes/DateType
    (instance? Timestamp value) DataTypes/TimestampType
    (nil? value) DataTypes/StringType ; Default for nil
    :else DataTypes/StringType)) ; Fallback to string

;; =====================================================================
;; Feature 4: Malli Schema Conversion
;; =====================================================================

(defn ^:private malli-type->data-type
  "Convert a Malli type to Snowpark DataType"
  [malli-type]
  (cond
    ;; Primitive types
    (= malli-type :int) DataTypes/IntegerType
    (= malli-type :string) DataTypes/StringType
    (= malli-type :double) DataTypes/DoubleType
    (= malli-type :boolean) DataTypes/BooleanType
    (= malli-type :uuid) DataTypes/StringType
    (= malli-type :keyword) DataTypes/StringType
    (= malli-type :symbol) DataTypes/StringType
    (= malli-type :nil) DataTypes/StringType
    (= malli-type :any) DataTypes/StringType
    
    ;; Date/time types
    (= malli-type :inst) DataTypes/TimestampType
    
    ;; Numeric types with constraints
    (and (vector? malli-type) (= (first malli-type) :int)) DataTypes/IntegerType
    (and (vector? malli-type) (= (first malli-type) :double)) DataTypes/DoubleType
    (and (vector? malli-type) (= (first malli-type) :string)) DataTypes/StringType
    
    ;; Collections default to string (JSON serialization)
    (or (= malli-type :vector) (= malli-type :set) (= malli-type :sequential)) DataTypes/StringType
    
    ;; Default fallback
    :else DataTypes/StringType))

(defn malli-schema->snowpark-schema
  "Convert a Malli schema to a Snowpark StructType schema.
   
   Supports map schemas like:
   [:map 
    [:id :int]
    [:name :string]
    [:active :boolean]]
   
   Args:
   - malli-schema: A Malli schema
   - write-key-fn: Function to transform field names when creating schema
    
   Returns a StructType that can be used with Snowpark DataFrames."
  [malli-schema write-key-fn]
  (when-not (m/schema? malli-schema)
    (throw (IllegalArgumentException. "Input must be a valid Malli schema")))
  
  (let [parsed (m/form malli-schema)]
    (when-not (and (vector? parsed) (= (first parsed) :map))
      (throw (IllegalArgumentException. "Only map schemas are supported for conversion to Snowpark schema")))
    
    (let [field-schemas (rest parsed)
          fields (for [[field-name field-type] field-schemas]
                   (let [snowpark-type (malli-type->data-type field-type)]
                     (StructField. (write-key-fn field-name) snowpark-type true)))]
      (StructType. (into-array StructField fields)))))

(defn infer-schema
  "Infer Snowpark schema from a collection of maps.
   
   Args:
   - maps: Collection of maps to infer schema from
   - write-key-fn: Function to transform keys when creating schema
   
   Returns a StructType representing the schema."
  [maps write-key-fn]
  (when (empty? maps)
    (throw (IllegalArgumentException. "Cannot infer schema from empty collection")))
  
  (let [sample-map (first maps)
        fields (for [[k v] sample-map]
                 (StructField. (write-key-fn k) (clojure-value->data-type v) true))]
    (StructType. (into-array StructField fields))))

(defn clojure-value->java
  "Convert a Clojure value to appropriate Java type for Snowpark"
  [value]
  (cond
    (keyword? value) (name value)
    (symbol? value) (name value)
    :else value))

(defn map->row
  "Convert a Clojure map to a Snowpark Row
   
   Args:
   - m: Clojure map to convert
   - schema: Snowpark schema
   - write-key-fn: Function to transform keys when matching schema fields"
  [m schema write-key-fn]
  (let [field-names (.names schema)
        ;; Create a case-insensitive lookup by normalizing both map keys and field names
        normalized-map (into {} (for [[k v] m]
                                  [(str/lower-case (write-key-fn k)) v]))
        lookup-value (fn [field-name]
                       (get normalized-map (str/lower-case field-name)))
        values (map #(clojure-value->java (lookup-value %)) field-names)
        values-array (into-array Object values)]
    (Row/create values-array)))

(defn maps->rows
  "Convert a vector of maps to an array of Snowpark Rows given a schema."
  [maps schema write-key-fn]
  (let [rows (map #(map->row % schema write-key-fn) maps)]
    (into-array com.snowflake.snowpark_java.Row rows)))

(defn row->map
  "Convert a Snowpark Row to a Clojure map"
  [row schema read-key-fn]
  (let [field-names (.names schema)
        field-count (count field-names)]
    (into {}
          (for [i (range field-count)
                :let [field-name (nth field-names i)
                      value (.get row i)]]
            [(read-key-fn field-name) value]))))

(defn rows->maps
  "Convert a collection of Snowpark Rows to a vector of maps"
  [rows schema read-key-fn]
  (mapv #(row->map % schema read-key-fn) rows))
