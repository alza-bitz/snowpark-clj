(ns snowpark-clj.convert-test
  "Unit tests for the convert namespace"
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.string :as str]
            [snowpark-clj.convert :as convert]
            [malli.core :as m]
            [malli.generator :as mg]))

;; Malli schema based on test_data.csv structure (Feature 4)
(def employee-schema
  (m/schema [:map
             [:id :int]
             [:name :string]
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]]))

;; =============================================================================
;; Individual tests for each public function
;; =============================================================================

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

(deftest test-malli-schema->snowpark-schema
  (testing "Converting Malli schema to Snowpark schema"
    (let [schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))]
      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY"] (vec (.names schema))))))
  
  (testing "Invalid input should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (convert/malli-schema->snowpark-schema {:not :a-schema} (comp str/upper-case name))))
    
    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (convert/malli-schema->snowpark-schema [:string] (comp str/upper-case name))))))

(deftest test-infer-schema
  (testing "Inferring schema from collection of maps with generated data"
    (let [employees (mg/generate [:vector {:gen/min 1 :gen/max 5} employee-schema] {:size 10})
          schema (convert/infer-schema employees (comp str/upper-case name))]
      ;; Test basic schema properties
      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY"] (vec (.names schema))))))
  
  (testing "Inferring schema from empty collection should throw exception"
    (is (thrown-with-msg? IllegalArgumentException 
                          #"Cannot infer schema from empty collection"
                          (convert/infer-schema [] (comp str/upper-case name))))))

(deftest test-map->row
  (testing "Converting map to row with generated data"
    (let [employee (mg/generate employee-schema {:size 10})
          schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))
          row (convert/map->row employee schema (comp str/upper-case name))]
      
      (is (some? row))
      (is (instance? com.snowflake.snowpark_java.Row row))
      
      ;; Test that we can get values back from the row
      (is (= (:id employee) (.get row 0)))
      (is (= (:name employee) (.get row 1)))
      (is (= (:department employee) (.get row 2)))
      (is (= (:salary employee) (.get row 3)))))
  
  (testing "Converting map with keyword keys"
    (let [test-map {:id 1 :name "Alice" :department "Engineering" :salary 75000}
          schema (convert/infer-schema [test-map] (comp str/upper-case name))
          row (convert/map->row test-map schema (comp str/upper-case name))]
      
      (is (some? row))
      (is (= 1 (.get row 0)))
      (is (= "Alice" (.get row 1))))))

(deftest test-maps->rows
  (testing "Converting multiple maps to rows array with generated data"
    (let [employees (mg/generate [:vector {:gen/min 2 :gen/max 5} employee-schema] {:size 10})
          schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))
          rows-array (convert/maps->rows employees schema (comp str/upper-case name))]
      
      (is (some? rows-array))
      (is (= (count employees) (count rows-array)))
      (is (every? #(instance? com.snowflake.snowpark_java.Row %) rows-array))
      
      ;; Test that first row matches first employee
      (let [first-row (first rows-array)
            first-employee (first employees)]
        (is (= (:id first-employee) (.get first-row 0)))
        (is (= (:name first-employee) (.get first-row 1)))))))

(deftest test-row->map
  (testing "Converting Row to map using real Snowpark objects with generated data"
    (let [employee (mg/generate employee-schema {:size 10})
          schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))
          row (convert/map->row employee schema (comp str/upper-case name))
          result (convert/row->map row schema (comp keyword str/lower-case))]
      
      (is (map? result))
      (is (= 4 (count result)))
      (is (= (:id employee) (:id result)))
      (is (= (:name employee) (:name result)))
      (is (= (:department employee) (:department result)))
      (is (= (:salary employee) (:salary result)))))
  
  (testing "Row to map conversion with different key transformation"
    (let [test-map {:id 1 :name "Alice" :department "Engineering" :salary 75000}
          schema (convert/infer-schema [test-map] (comp str/upper-case name))
          row (convert/map->row test-map schema (comp str/upper-case name))
          result (convert/row->map row schema (comp keyword str/lower-case))]
      
      (is (map? result))
      (is (= 4 (count result)))
      (is (= 1 (:id result)))
      (is (= "Alice" (:name result)))
      (is (= "Engineering" (:department result)))
      (is (= 75000 (:salary result))))))

(deftest test-rows->maps
  (testing "Converting collection of Rows to vector of maps with generated data"
    (let [employees (mg/generate [:vector {:gen/min 2 :gen/max 5} employee-schema] {:size 10})
          schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))
          rows-array (convert/maps->rows employees schema (comp str/upper-case name))
          result-maps (convert/rows->maps rows-array schema (comp keyword str/lower-case))]
      
      (is (vector? result-maps))
      (is (= (count employees) (count result-maps)))
      (is (every? map? result-maps))
      
      ;; Test that the conversion is round-trip safe
      (is (= (set (map :id employees)) (set (map :id result-maps))))
      (is (= (set (map :name employees)) (set (map :name result-maps)))))))

;; =============================================================================
;; Property-based tests
;; =============================================================================

;; Property-based round-trip test (Feature 4)
(defspec roundtrip-malli-schema-property 20
  (prop/for-all [employees (mg/generator [:vector {:gen/max 10} employee-schema] {:size 10})]
    (let [schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))
          rows (convert/maps->rows employees schema (comp str/upper-case name))]
      (= employees (convert/rows->maps rows schema (comp keyword str/lower-case))))))

;; Property-based test for individual map/row conversion
(defspec map-row-roundtrip-property 20
  (prop/for-all [employee (mg/generator employee-schema {:size 10})]
    (let [schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))
          row (convert/map->row employee schema (comp str/upper-case name))
          result (convert/row->map row schema (comp keyword str/lower-case))]
      (= employee result))))

;; Property-based test for clojure-value->java conversion
(defspec clojure-value-conversion-property 20
  (prop/for-all [value (mg/generator [:or :int :string :boolean :keyword :symbol] {:size 10})]
    (let [result (convert/clojure-value->java value)]
      (cond
        (keyword? value) (= result (name value))
        (symbol? value) (= result (name value))
        :else (= result value)))))

;; Property-based test for schema inference consistency
(defspec schema-inference-consistency-property 20
  (prop/for-all [employees (mg/generator [:vector {:gen/min 1 :gen/max 10} employee-schema] {:size 10})]
    (let [inferred-schema (convert/infer-schema employees (comp str/upper-case name))
          malli-schema (convert/malli-schema->snowpark-schema employee-schema (comp str/upper-case name))]
      ;; Both schemas should have the same field names
      (= (vec (.names inferred-schema)) (vec (.names malli-schema))))))
