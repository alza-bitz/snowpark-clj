(ns snowpark-clj.config-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.properties :as prop]
   [malli.core :as m]
   [malli.generator :as mg]
   [mask.core :as mask]
   [snowpark-clj.config :as config]))

(defspec mask-schema-generated-values-are-valid-property 20
  (prop/for-all [masked-value (mg/generator config/mask-schema {:size 10})]
                (is (m/validate config/mask-schema masked-value))))

(defspec mask-schema-generated-values-are-invalid-if-unmasked-property 20
  (prop/for-all [masked-value (mg/generator config/mask-schema {:size 10})]
                (is (not (m/validate config/mask-schema (mask/unmask masked-value))))))

(deftest test-mask-schema
  
  (testing "Nil values are invalid"
    (is (not (m/validate config/mask-schema nil))))
  
  (testing "Masked empty string values are invalid"
    (is (not (m/validate config/mask-schema (mask/mask "")))))
  
  (testing "Masked non-string values are invalid"
    (is (not (m/validate config/mask-schema (mask/mask 1))))))

(deftest test-config-schema
  
  (testing "Config with missing required keys is invalid"
    (let [invalid-config (dissoc (mg/generate config/config-schema) :url)]
      (is (not (m/validate config/config-schema invalid-config)))))

  (testing "Config with extra keys is invalid"
    (let [invalid-config (assoc (mg/generate config/config-schema) :extra "not-defined-in-schema")]
      (is (not (m/validate config/config-schema invalid-config)))))

  (testing "Config with nil string keys is invalid"
    (doseq [config-key [:url :role]]
      (let [invalid-config (assoc (mg/generate config/config-schema) config-key nil)]
        (is (not (m/validate config/config-schema invalid-config))))))

  (testing "Config with empty string keys is invalid"
    (doseq [config-key [:url :role]]
      (let [invalid-config (assoc (mg/generate config/config-schema) config-key "")]
        (is (not (m/validate config/config-schema invalid-config))))))
  
    (testing "Config with nil masked string keys throws validation? error"
    (doseq [config-key [:password]]
      (let [invalid-config (assoc (mg/generate config/config-schema) config-key nil)]
        (is (not (m/validate config/config-schema invalid-config))))))
  
  (testing "Config with empty masked string keys is invalid"
    (doseq [config-key [:password]]
      (let [invalid-config (assoc (mg/generate config/config-schema) config-key (mask/mask ""))]
        (is (not (m/validate config/config-schema invalid-config))))))
  
  (testing "Config with non-masked masked string keys is invalid"
    (doseq [config-key [:password]]
      (let [invalid-config (update (mg/generate config/config-schema) config-key mask/unmask)]
        (is (not (m/validate config/config-schema invalid-config)))))))