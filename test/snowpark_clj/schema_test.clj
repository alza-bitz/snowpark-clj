(ns snowpark-clj.schema-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.properties :as prop]
   [malli.core :as m]
   [malli.generator :as mg]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.wrapper :as wrapper]
   [spy.protocol :as protocol]))

;; Malli schema based on test_data.csv structure (Feature 4)
(def employee-schema
  (m/schema [:map
             [:id :int]
             [:name :string]
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]]))

(defn- mock-session-wrapper [write-key-fn]
  (let [mock (protocol/mock wrapper/IWrappedSessionOptions
                            (unwrap-option [_ _] write-key-fn))]
    {:mock-session-wrapper mock
     :mock-session-wrapper-spies (protocol/spies mock)}))

(deftest test-infer-schema
  (testing "Inferring schema from collection of maps with generated data"
    (let [employees (mg/generate [:vector {:gen/min 1 :gen/max 5} employee-schema] {:size 10})
          write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)
          schema (schema/infer-schema mock-session-wrapper employees)]
      ;; Test basic schema properties
      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY"] (vec (.names schema))))))

  (testing "Inferring schema from empty collection should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot infer schema from empty collection"
                          (schema/infer-schema (mock-session-wrapper (comp str/upper-case name)) [])))))

(deftest test-malli-schema->snowpark-schema
  (testing "Converting Malli schema to Snowpark schema"
    (let [write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)
          schema (schema/malli-schema->snowpark-schema mock-session-wrapper employee-schema)]
      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY"] (vec (.names schema))))))

  (testing "Invalid input should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (schema/malli-schema->snowpark-schema {} {:not :a-schema})))

    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (schema/malli-schema->snowpark-schema {} [:string])))
    
    (is (thrown-with-msg? IllegalArgumentException
                          #"Only map schemas are supported"
                          (schema/malli-schema->snowpark-schema {} (m/schema :string))))))

;; Property-based test for schema inference consistency
(defspec schema-inference-consistency-property 20
  (prop/for-all [employees (mg/generator [:vector {:gen/min 1 :gen/max 10} employee-schema] {:size 10})]
    (let [write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn) 
          inferred-schema (schema/infer-schema mock-session-wrapper employees)
          malli-schema (schema/malli-schema->snowpark-schema mock-session-wrapper employee-schema)]
      ;; Both schemas should have the same field names
      (= (vec (.names inferred-schema)) (vec (.names malli-schema))))))
