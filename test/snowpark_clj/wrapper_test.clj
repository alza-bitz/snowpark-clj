(ns snowpark-clj.wrapper-test 
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [snowpark-clj.mocks :as mocks]
   [snowpark-clj.wrapper :as wrapper]
   [spy.assert :as assert]))

(deftest test-wrap-session
  (testing "Wrap session"
    (let [mock-session {:mock true}
          session-wrapper (wrapper/wrap-session mock-session {})
          result (wrapper/unwrap session-wrapper)]
      (is (= mock-session result))))
  
  (testing "TODO Extracting option from wrapper")

  (testing "TODO Extracting all options from wrapper"))

;; (deftest test-unwrap-read-key-fn
;;   (testing "Extracting read-key-fn from wrapper"
;;     (let [custom-read-key-fn keyword
;;           session-wrapper {:session {:mock true} :read-key-fn custom-read-key-fn}
;;           result (session/unwrap-read-key-fn session-wrapper)]
;;       (is (= custom-read-key-fn result)))))

;; (deftest test-unwrap-write-key-fn
;;   (testing "Extracting write-key-fn from wrapper"
;;     (let [custom-write-key-fn name
;;           session-wrapper {:session {:mock true} :write-key-fn custom-write-key-fn}
;;           result (session/unwrap-write-key-fn session-wrapper)]
;;       (is (= custom-write-key-fn result)))))

(deftest test-wrap-dataframe
  (testing "DataFrame wrapper supports map-like column access (feature 5)"
    (let [{:keys [mock-schema]} (mocks/mock-schema ["NAME" "SALARY" "AGE" "DEPARTMENT" "ID"])
          {:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe {:mock-schema mock-schema})
          test-df (wrapper/wrap-dataframe mock-dataframe {:write-key-fn (comp str/upper-case name)
                                                       :read-key-fn (comp keyword str/lower-case)})]

      (testing "IFn access: (df :column)"
        (let [result (test-df :name)]
          (is (some? result))
          ;; Verify the column name decoding was applied before calling .col
          (assert/called-once? (:col mock-dataframe-spies))
          (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "NAME")))

      (testing "ILookup access: (:column df)"
        (let [result (:salary test-df)]
          (is (some? result))
          ;; Check that .col was called again (2nd time total)
          (assert/called-n-times? (:col mock-dataframe-spies) 2)
          (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")))

      (testing "Non-existent column returns nil"
        ;; Test both access patterns return nil for non-existent columns
        (is (nil? (test-df :non-existent)))
        (is (nil? (:also-non-existent test-df))))

      (testing "IPersistentCollection access: (count df) returns number of fields"
        (let [field-count (clojure.core/count test-df)]
          (is (= 5 field-count))))

      (testing "Iterable/Seqable access: (keys df) returns seq of encoded field names"
        (let [df-keys (keys test-df)]
          (is (= 5 (clojure.core/count df-keys)))
          ;; Keys should be encoded using read-key-fn (keyword + lowercase)
          (is (= #{:name :salary :age :department :id} (set df-keys)))))

      (testing "Iterable/Seqable access: (vals df) returns seq of column objects"
        (let [df-vals (vals test-df)]
          (is (= 5 (clojure.core/count df-vals)))
          ;; Each value should be a Column object
          (is (every? #(instance? com.snowflake.snowpark_java.Column %) df-vals))))

      (testing "Iterable/Seqable access: (seq df) returns seq of MapEntry, each with the encoded field name and column object"
        (let [df-seq (seq test-df)]
          (is (= 5 (clojure.core/count df-seq)))
          ;; Each item should be a MapEntry object
          (is (every? #(instance? clojure.lang.MapEntry %) df-seq))
          ;; First element (key) should be keyword, second element (val) should be Column object
          (is (every? keyword? (map key df-seq)))
          (is (every? #(instance? com.snowflake.snowpark_java.Column %) (map val df-seq)))
          ;; Check that we have the expected keys
          (is (= #{:name :salary :age :department :id} (set (map key df-seq))))))

      (testing "Printing to a string"
        (let [df-str (pr-str test-df)]
          (is (str/includes? df-str "COL")))))))
