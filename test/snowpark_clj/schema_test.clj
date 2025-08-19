(ns snowpark-clj.schema-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.properties :as prop]
   [malli.core :as m]
   [malli.generator :as mg]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.schemas :as schemas]
   [snowpark-clj.wrapper :as wrapper]
   [spy.protocol :as protocol])
  (:import
   [com.snowflake.snowpark_java.types DataTypes]))

(defn- mock-session-wrapper [write-key-fn]
  (let [mock (protocol/mock wrapper/IWrappedSessionOptions
                            (unwrap-option [_ _] write-key-fn))]
    {:mock-session-wrapper mock
     :mock-session-wrapper-spies (protocol/spies mock)}))

(deftest test-infer-schema
  (testing "Infer schema with no optional keys in schema"
    (let [employees (mg/generate [:vector {:gen/min 1 :gen/max 5} schemas/employee-schema])
          write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)
          schema (schema/infer-schema mock-session-wrapper employees)]

      (is (some? schema))
      (is (= 4 (count (.names schema))))
      (doseq [[expected-name expected-type expected-nullable field]
              (map list
                   ["ID" "NAME" "DEPARTMENT" "SALARY"]
                   [DataTypes/IntegerType DataTypes/StringType DataTypes/StringType DataTypes/IntegerType]
                   (repeat true)
                   (iterator-seq (.iterator schema)))]
        
        (is (= expected-name (.name field)) (str "Name mismatch for " expected-name))
        (is (= expected-type (.dataType field)) (str "Data type mismatch for field " expected-name))
        (is (= expected-nullable (.nullable field)) (str "Nullable mismatch for field " expected-name)))))

  (testing "Infer schema with optional keys present as nil values"
    (let [employees (mg/generate [:vector {:gen/min 1 :gen/max 5} schemas/employee-schema-with-nil-values])
          write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)
          schema (schema/infer-schema mock-session-wrapper employees)]

      (is (some? schema))
      (is (= 5 (count (.names schema))))
      (is (= ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"] (vec (.names schema))))))

  (testing "Infer schema with empty collection should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot infer schema from empty collection"
                          (schema/infer-schema (mock-session-wrapper (comp str/upper-case name)) [])))))

(deftest test-malli-schema->snowpark-schema
  (testing "Create schema from Malli schema with optional keys"
    (let [write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)
          schema (schema/malli-schema->snowpark-schema mock-session-wrapper schemas/employee-schema-with-optional-keys)]

      (is (some? schema))
      (is (= 5 (count (.names schema))))
      (doseq [[expected-name expected-type expected-nullable field]
              (map list
                   ["ID" "NAME" "DEPARTMENT" "SALARY" "AGE"]
                   [DataTypes/IntegerType DataTypes/StringType DataTypes/StringType DataTypes/IntegerType DataTypes/IntegerType]
                   [false false false false true]
                   (iterator-seq (.iterator schema)))]

        (is (= expected-name (.name field)) (str "Name mismatch for " expected-name))
        (is (= expected-type (.dataType field)) (str "Data type mismatch for " expected-name))
        (is (= expected-nullable (.nullable field)) (str "Nullable mismatch for " expected-name)))))

  (testing "Create schema from Malli schema with invalid inputs"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (schema/malli-schema->snowpark-schema {} {:not :a-schema})))

    (is (thrown-with-msg? IllegalArgumentException
                          #"Input must be a valid Malli schema"
                          (schema/malli-schema->snowpark-schema {} [:string])))

    (is (thrown-with-msg? IllegalArgumentException
                          #"Only map schemas are supported"
                          (schema/malli-schema->snowpark-schema {} (m/schema :string)))) 

    (let [write-key-fn (comp str/upper-case name)
          {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)]
      (is (thrown-with-msg? IllegalArgumentException
                          #"Unsupported schema"
                          (schema/malli-schema->snowpark-schema mock-session-wrapper schemas/employee-schema-with-maybe-nil-values))))))

;; Property-based test for schema inference consistency with no optional keys in schema
(defspec schema-inference-consistency-property 20
  (prop/for-all [employees (mg/generator [:vector {:gen/min 1 :gen/max 10} schemas/employee-schema])]
                (let [write-key-fn (comp str/upper-case name)
                      {:keys [mock-session-wrapper]} (mock-session-wrapper write-key-fn)
                      inferred-schema (schema/infer-schema mock-session-wrapper employees)
                      converted-schema (schema/malli-schema->snowpark-schema mock-session-wrapper schemas/employee-schema)]
                  ;; Both schemas should have the same field names in the same order
                  (= (vec (.names inferred-schema)) (vec (.names converted-schema))))))
