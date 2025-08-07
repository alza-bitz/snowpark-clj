(ns snowpark-clj.schema
  (:require
   [malli.core :as m]
   [snowpark-clj.session :as session]) 
  (:import
   [com.snowflake.snowpark_java.types DataTypes StructField StructType]
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

(defn infer-schema
  "Infer Snowpark schema from a collection of maps.
   
   Args:
   - session: Snowpark session wrapper including :write-key-fn for decoding schema field names
   - maps: Collection of maps to infer schema from
   
   Returns a StructType representing the schema."
  [session maps]
  (when (empty? maps)
    (throw (IllegalArgumentException. "Cannot infer schema from empty collection")))

  (let [write-key-fn (session/get-write-key-fn session)
        sample-map (first maps)
        fields (for [[k v] sample-map]
                 (StructField. (write-key-fn k) (clojure-value->data-type v) true))]
    (StructType. (into-array StructField fields))))


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
   - session: Snowpark session wrapper including :write-key-fn for decoding schema field names
   - malli-schema: Malli schema
    
   Returns a StructType that can be used with Snowpark DataFrames."
  [session malli-schema]
  (when-not (m/schema? malli-schema)
    (throw (IllegalArgumentException. "Input must be a valid Malli schema")))
  
  (let [parsed (m/form malli-schema)]
    (when-not (and (vector? parsed) (= (first parsed) :map))
      (throw (IllegalArgumentException. "Only map schemas are supported for conversion to Snowpark schema")))
    
    (let [field-schemas (rest parsed)
          write-key-fn (session/get-write-key-fn session)
          fields (for [[field-name field-type] field-schemas]
                   (let [snowpark-type (malli-type->data-type field-type)]
                     (StructField. (write-key-fn field-name) snowpark-type true)))]
      (StructType. (into-array StructField fields)))))

