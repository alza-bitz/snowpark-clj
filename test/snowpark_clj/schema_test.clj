(ns snowpark-clj.schema-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.string :as str]
            [snowpark-clj.schema :as schema]
            [malli.core :as m]
            [malli.generator :as mg]))

;; Malli schema based on test_data.csv structure (Feature 4)
(def employee-schema
  (m/schema [:map
             [:id :int]
             [:name :string]
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]]))

(deftest test-infer-schema
  (testing "Inferring schema from collection of maps with generated data"
    (let [employees (mg/generate [:vector {:gen/min 1 :gen/max 5} employee-schema] {:size 10})
          session {:write-key-fn (comp str/upper-case name)}
          schema (schema/infer-schema session employees)]
      ;; Test basic schema properties
      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY"] (vec (.names schema))))))

  (testing "Inferring schema from empty collection should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot infer schema from empty collection"
                          (schema/infer-schema {:write-key-fn (comp str/upper-case name)} [])))))

(deftest test-malli-schema->snowpark-schema
  (testing "Converting Malli schema to Snowpark schema"
    (let [session {:write-key-fn (comp str/upper-case name)}
          schema (schema/malli-schema->snowpark-schema session employee-schema)]
      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY"] (vec (.names schema))))))

  (testing "Invalid input should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (schema/malli-schema->snowpark-schema {:write-key-fn (comp str/upper-case name)} {:not :a-schema})))

    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (schema/malli-schema->snowpark-schema {:write-key-fn (comp str/upper-case name)} [:string])))))

;; Property-based test for schema inference consistency
(defspec schema-inference-consistency-property 20
  (prop/for-all [employees (mg/generator [:vector {:gen/min 1 :gen/max 10} employee-schema] {:size 10})]
    (let [session {:write-key-fn (comp str/upper-case name)}
          inferred-schema (schema/infer-schema session employees)
          malli-schema (schema/malli-schema->snowpark-schema session employee-schema)]
      ;; Both schemas should have the same field names
      (= (vec (.names inferred-schema)) (vec (.names malli-schema))))))
