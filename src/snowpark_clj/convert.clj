(ns snowpark-clj.convert
  "Data conversion utilities between Clojure and Snowpark"
  (:require [clojure.string :as str])
  (:import [com.snowflake.snowpark_java Row]))

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
   - data: Clojure map to convert
   - schema: Snowpark schema
   - write-key-fn: Function to decode keys when matching schema fields"
  [data schema write-key-fn]
  (let [field-names (.names schema)
        ;; Create a case-insensitive lookup by normalizing both map keys and field names
        normalized-map (into {} (for [[k v] data]
                                  [(str/lower-case (write-key-fn k)) v]))
        lookup-value (fn [field-name]
                       (get normalized-map (str/lower-case field-name)))
        values (map #(clojure-value->java (lookup-value %)) field-names)
        values-array (into-array Object values)]
    (Row/create values-array)))

(defn maps->rows
  "Convert a vector of maps to an array of Snowpark Rows given a schema."
  [data schema write-key-fn]
  (let [rows (map #(map->row % schema write-key-fn) data)]
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
            (when value [(read-key-fn field-name) value])))))

(defn rows->maps
  "Convert a collection of Snowpark Rows to a vector of maps"
  [rows schema read-key-fn]
  (mapv #(row->map % schema read-key-fn) rows))
