(ns snowpark-clj.wrapper-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [snowpark-clj.mocks :as mocks]
   [snowpark-clj.wrapper :as wrapper]
   [spy.assert :as assert]))

(deftest test-wrap-session
  
  (testing "Wrap session for session and dataframe is wrapped"
    (let [mock-session {:mock true}
          mock-dataframe {:mock true}]
      (doseq [wrapper [(wrapper/wrap-session mock-session {:read-key-fn identity})
                       (wrapper/wrap-dataframe mock-dataframe {:read-key-fn identity})]]
        (is (wrapper/wrapper? wrapper)))))

  (testing "Wrap session for session and dataframe implements IWrappedSessionOptions"
    (let [mock-session {:mock true}
          mock-dataframe {:mock true}]
      (doseq [mock [mock-session mock-dataframe]
              wrapper [(wrapper/wrap-session mock-session {:read-key-fn identity})
                       (wrapper/wrap-dataframe mock-dataframe {:read-key-fn identity})]]

        (is (= mock (wrapper/unwrap wrapper)))
        (is (= identity (wrapper/unwrap-option wrapper :read-key-fn)))
        (is (= {:read-key-fn identity} (wrapper/unwrap-options wrapper)))))))

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
