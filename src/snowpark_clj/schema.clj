(ns snowpark-clj.schema
  (:require
   [malli.core :as m]
   [snowpark-clj.wrapper :as wrapper]) 
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
  "Infer a Snowpark schema from a collection of maps.
   This implementation only looks at the first map, and the order of inferred fields 
   will be determined by (seq (first data)). All inferred fields will be nullable.
   
   Args:
   - session: Session wrapper including :write-key-fn for decoding schema field names
   - data: Collection of maps representing rows to infer schema from
   
   Returns a StructType representing the schema."
  [session data]
  (when (empty? data)
    (throw (IllegalArgumentException. "Cannot infer schema from empty collection")))

  (let [write-key-fn (wrapper/unwrap-option session :write-key-fn)
        sample-map (first data)
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

    ;; Types with constraints
    (and (vector? malli-type) (= (first malli-type) :int)) DataTypes/IntegerType
    (and (vector? malli-type) (= (first malli-type) :double)) DataTypes/DoubleType
    (and (vector? malli-type) (= (first malli-type) :string)) DataTypes/StringType

    ;; Enums
    (and (vector? malli-type) (= (first malli-type) :enum) (every? int? (rest malli-type))) DataTypes/IntegerType
    (and (vector? malli-type) (= (first malli-type) :enum) (every? double? (rest malli-type))) DataTypes/DoubleType
    (and (vector? malli-type) (= (first malli-type) :enum) (every? string? (rest malli-type))) DataTypes/StringType

    ;; Default fallback
    :else (throw (IllegalArgumentException. (str "Unsupported schema: " malli-type)))))

(defn malli-schema->snowpark-schema
  "Create a Snowpark schema from a Malli schema.
   
   Supports map schemas like:
   [:map 
    [:id :int]
    [:name :string]
    [:active :boolean]]
   
   Args:
   - session: Session wrapper including :write-key-fn for decoding schema field names
   - malli-schema: Malli schema
    
   Returns a StructType that can be used with Snowpark DataFrames."
  [session malli-schema]
  (when-not (m/schema? malli-schema)
    (throw (IllegalArgumentException. "Input must be a valid Malli schema")))
  
  (let [parsed (m/form malli-schema)]
    (when-not (and (vector? parsed) (= (first parsed) :map))
      (throw (IllegalArgumentException. "Only map schemas are supported"))) 
    (let [field-schemas (rest parsed)
          fields (for [[field-name & field-rest] field-schemas] 
                   (let [field-type (if (= 2 (count field-rest))
                                      (second field-rest)
                                      (first field-rest))
                         field-options (if (= 2 (count field-rest))
                                         (first field-rest)
                                         {})
                         snowpark-type (malli-type->data-type field-type)
                         write-key-fn (wrapper/unwrap-option session :write-key-fn)]
                     (StructField. (write-key-fn field-name) snowpark-type (or (:optional field-options) false))))]
      (StructType. (into-array StructField fields)))))

