(ns snowpark-clj.convert-test
  "Unit tests for the convert namespace"
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.properties :as prop]
   [malli.generator :as mg]
   [snowpark-clj.convert :as convert]
   [snowpark-clj.mocks :as mocks]
   [snowpark-clj.schemas :as schemas])
  (:import
   [com.snowflake.snowpark_java Row]))

(deftest test-clojure-value->java
  (testing "Converting Clojure values to Java types"
    ;; Test with generated values using property-based approach
    (is (= "test" (convert/clojure-value->java :test)))
    (is (= "symbol" (convert/clojure-value->java 'symbol)))
    (is (= "string" (convert/clojure-value->java "string")))
    (is (= 42 (convert/clojure-value->java 42)))
    (is (= true (convert/clojure-value->java true)))
    (is (nil? (convert/clojure-value->java nil))))

  (testing "Edge cases"
    (is (= "" (convert/clojure-value->java (keyword ""))))
    (is (= "some-keyword" (convert/clojure-value->java :some-keyword)))))

(deftest test-map->row
  (testing "Converting map to row with no optional keys in schema"
    (let [employee (mg/generate schemas/employee-schema)
          key->col-fn (comp str/upper-case name)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY"])
          result (convert/map->row employee mock-schema key->col-fn)]

      (is (some? result))
      (is (instance? com.snowflake.snowpark_java.Row result))
      (is (= 4 (.size result)))
      (doseq [[key index] (map list [:id :name :department :salary] (range))]
        (is (= (get employee key) (.get result index)) (str "Value mismatch for " key)))))

  (testing "Converting map to row with optional keys missing"
    (let [data (mg/generate schemas/employee-schema)
          key->col-fn (comp str/upper-case name)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
          result (convert/map->row data mock-schema key->col-fn)]

      (is (some? result))
      (is (instance? com.snowflake.snowpark_java.Row result))
      (is (= 5 (.size result)))
      (doseq [[key index] (map list [:id :name :department :salary :age] (range))]
        (is (= (get data key) (.get result index)) (str "Value mismatch for " key)))))
  
  (testing "Converting map to row with optional keys present as nil values"
    (let [data (mg/generate schemas/employee-schema-with-nil-values)
          key->col-fn (comp str/upper-case name)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
          row (convert/map->row data mock-schema key->col-fn)]
    
      (is (some? row))
      (is (instance? com.snowflake.snowpark_java.Row row))
      (is (= 5 (.size row)))
      (doseq [[key index] (map list [:id :name :department :salary :age] (range))]
        (is (= (get data key) (.get row index)) (str "Value mismatch for " key)))))
  
  (testing "Converting map to row with extra keys not in schema"
    (let [data (mg/generate schemas/employee-schema)
          key->col-fn (comp str/upper-case name)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
          result (convert/map->row (assoc data :extra "not-in-schema") mock-schema key->col-fn)]
    
      (is (some? result))
      (is (instance? com.snowflake.snowpark_java.Row result))
      (is (= 5 (.size result))))))

(deftest test-maps->rows
  (testing "Converting maps to rows with optional keys either present or missing"
    (let [data (mg/generate [:vector {:gen/min 2 :gen/max 5} schemas/employee-schema-with-optional-keys])
          key->col-fn (comp str/upper-case name)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
          result (convert/maps->rows data mock-schema key->col-fn)]

      (is (some? result))
      (is (= (count data) (count result)))
      (is (every? #(instance? com.snowflake.snowpark_java.Row %) result))
      (is (every? #(= 5 (.size %)) result)))))

(deftest test-row->map
  (testing "Converting row to map with non-null values for all schema fields"
    (let [employee (mg/generate schemas/employee-schema)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY"])
          row (Row/create (into-array Object (vals employee)))
          result (convert/row->map row mock-schema (comp keyword str/lower-case))]

      (is (map? result))
      (is (= 4 (count result)))
      (is (= (keys employee) (keys result)))
      (is (= employee result))))

  (testing "Converting row to map with null values for nullable fields"
    (let [employee (mg/generate schemas/employee-schema-with-nil-values)
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
          row (Row/create (into-array Object (vals employee)))
          result (convert/row->map row mock-schema (comp keyword str/lower-case))]
      
      (is (map? result))
      (is (= 4 (count result)))
      (is (= (keys (dissoc employee :age)) (keys result)))
      (is (= (dissoc employee :age) result)))))

(deftest test-rows->maps
  (testing "Converting rows to maps with with null or non-null values for nullable fields"
    (let [employees (mg/generate [:vector {:gen/min 2 :gen/max 5} schemas/employee-schema-with-maybe-nil-values])
          {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
          rows (into-array Row (mapv #(Row/create (into-array Object (vals %))) employees))
          result (convert/rows->maps rows mock-schema (comp keyword str/lower-case))]

      (is (vector? result))
      (is (= (count employees) (count result)))
      (is (every? map? result))
      (is (every? #(or (= 4 (count %)) (= 5 (count %))) result)))))

;; =============================================================================
;; Property-based tests
;; =============================================================================

;; Property-based round-trip test (Feature 4)
(defspec maps-rows-roundtrip-property 20
  (prop/for-all [employees (mg/generator [:vector {:gen/max 10} schemas/employee-schema-with-optional-keys])]
                (let [key->col-fn (comp str/upper-case name)
                      {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
                      rows (convert/maps->rows employees mock-schema key->col-fn)]
                  (= employees (convert/rows->maps rows mock-schema (comp keyword str/lower-case))))))

;; Property-based test for individual map/row conversion
(defspec map-row-roundtrip-property 20
  (prop/for-all [employee (mg/generator schemas/employee-schema-with-optional-keys)]
                (let [key->col-fn (comp str/upper-case name)
                      {:keys [mock-schema]} (mocks/mock-schema ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"])
                      row (convert/map->row employee mock-schema key->col-fn)]
                  (= employee (convert/row->map row mock-schema (comp keyword str/lower-case))))))

;; Property-based test for clojure-value->java conversion
(defspec clojure-value-conversion-property 20
  (prop/for-all [value (mg/generator [:or :int :string :boolean :keyword :symbol])]
                (let [result (convert/clojure-value->java value)]
                  (cond
                    (keyword? value) (= result (name value))
                    (symbol? value) (= result (name value))
                    :else (= result value)))))
