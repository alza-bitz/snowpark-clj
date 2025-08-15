(ns snowpark-clj.integration-test
  "Integration tests for the snowpark-clj library"
  (:require 
   [clojure.test :refer [deftest is testing use-fixtures]]
   [malli.core :as m]
   [malli.generator :as mg]
   [snowpark-clj.core :as sp] 
   [snowpark-clj.dataframe :as df]))

;; Test data and schema
(def test-data
  [{:id 1 :name "Alice" :age 25 :department "Engineering" :salary 70000}
   {:id 2 :name "Bob" :age 30 :department "Engineering" :salary 80000}
   {:id 3 :name "Charlie" :age 35 :department "Sales" :salary 60000}])

;; Malli schema for test data (Feature 4)
(def test-employee-schema
  (m/schema [:map
             [:id :int]
             [:name :string]
             [:age :int]
             [:department [:enum "Engineering" "Sales" "Marketing"]]
             [:salary [:int {:min 50000 :max 100000}]]]))

;; Test fixtures
(def ^:dynamic *session* nil)
(def test-table-name "SNOWPARK_CLJ_TEST_TABLE")

(defn session-fixture
  "Create a session for testing and clean up afterwards"
  [f]
  (let [session (sp/create-session "integration/snowflake.edn")]
    (try
      ;; Clean up any existing test table before running test
      (try
        (sp/sql session (str "DROP TABLE IF EXISTS " test-table-name))
        (catch Exception e
          (println "Warning: Could not drop table:" (.getMessage e))))
      (binding [*session* session] 
        (f))
      (finally
        ;; Clean up: drop test table and close session
        (try
          (sp/sql session (str "DROP TABLE IF EXISTS " test-table-name))
          (catch Exception e
            (println "Warning: Could not drop table during cleanup:" (.getMessage e))))
        (try
          (sp/close-session session)
          (catch Exception e
            (println "Warning: Could not close session:" (.getMessage e))))))))

(use-fixtures :each session-fixture)

;; Feature 1 Integration Tests
(deftest test-feature-1-create-and-save-dataframe
  (testing "Feature 1: Load Clojure data and save to Snowflake table"
    ;; Create DataFrame from Clojure data
    (let [dataframe (sp/create-dataframe *session* test-data)]
      
      ;; Verify DataFrame was created successfully
      (is (some? dataframe))
      (is (map? dataframe))
      (is (df/unwrap-dataframe dataframe))
      (is (df/unwrap-option dataframe :read-key-fn))
      (is (df/unwrap-option dataframe :write-key-fn))
      
      ;; Check schema
      (let [schema (sp/schema dataframe)]
        (is (some? schema)))
      
      ;; Check columns
      (let [columns (.names (sp/schema dataframe))]
        (is (= 5 (count columns)))
        (is (every? #{"ID" "NAME" "AGE" "DEPARTMENT" "SALARY"} columns)))
      
      ;; Save to Snowflake table
      (sp/save-as-table dataframe test-table-name :overwrite)
      
      ;; Verify table was created by reading it back
      (let [table-df (sp/table *session* test-table-name)
            results (sp/collect table-df)]
        (is (= 3 (count results)))
        (is (every? map? results))
        
        ;; Verify data integrity
        (let [sorted-results (sort-by :id results)]
          (is (= 1 (:id (first sorted-results))))
          (is (= "Alice" (:name (first sorted-results))))
          (is (= 25 (:age (first sorted-results))))
          (is (= "Engineering" (:department (first sorted-results))))
          (is (= 70000 (:salary (first sorted-results)))))))))

;; Feature 2 Integration Tests  
(deftest test-feature-2-read-and-compute
  (testing "Feature 2: Read from table, compute, and collect results"
    ;; First create and save test data (Feature 1)
    (let [dataframe (sp/create-dataframe *session* test-data)]
      (sp/save-as-table dataframe test-table-name :overwrite)
      
      ;; Read from table and apply transformations
      (let [table-df (sp/table *session* test-table-name)
            salary-col (sp/col table-df :salary)
            salary-condition (sp/gt salary-col (sp/lit 65000))
            filtered-df (sp/filter table-df salary-condition)
            selected-df (sp/select filtered-df [:name :salary])
            sorted-df (sp/sort selected-df [:salary])
            limited-df (sp/limit sorted-df 2)
            results (sp/collect limited-df)]
        
        (is (vector? results))
        (is (<= (count results) 2))
        
        ;; Verify data structure (keys should be keywordized due to read-key-fn)
        (when (seq results)
          (let [first-row (first results)]
            (is (map? first-row))
            (is (contains? first-row :name))  ; Lowercase from read-key-fn conversion
            (is (contains? first-row :salary)))))
      
      ;; Test other action operations
      (let [table-df (sp/table *session* test-table-name)
            row-count (sp/count table-df)]
        (is (= 3 row-count)))
       
      (let [table-df (sp/table *session* test-table-name)
            sample-rows (sp/take table-df 2)]
        (is (= 2 (count sample-rows)))
        (is (every? map? sample-rows))))))

;; Feature 3 Integration Tests (Session Macros)
(deftest test-feature-3-session-macros
  (testing "Feature 3: Session macros work correctly"
    ;; Test with-session macro
    (let [result (sp/with-session [session (sp/create-session "integration/snowflake.edn")]
                   (let [df (sp/create-dataframe session test-data)]
                     (sp/count df)))]
      (is (= 3 result)))))

;; Feature 4 Integration Tests (Malli Schema Conversion)
(deftest test-feature-4-malli-schema-conversion
  (testing "Feature 4: Malli schema to Snowpark schema conversion"
    ;; Convert Malli schema to Snowpark schema
    (let [snowpark-schema (sp/malli-schema->snowpark-schema *session* test-employee-schema)]
      (is (some? snowpark-schema))

      ;; Create DataFrame with explicit schema
      (let [dataframe (sp/create-dataframe *session* test-data snowpark-schema)]
        (is (some? dataframe))
        (is (map? dataframe))

        ;; Verify schema matches
        (let [columns (.names (sp/schema dataframe))]
          (is (= 5 (count columns)))
          (is (every? #{"ID" "NAME" "AGE" "DEPARTMENT" "SALARY"} columns)))))

    ;; Test with generated data
    (let [generated-data (mg/generate [:vector {:gen/min 5 :gen/max 5} test-employee-schema])
          snowpark-schema (sp/malli-schema->snowpark-schema *session* test-employee-schema)
          dataframe (sp/create-dataframe *session* generated-data snowpark-schema)]

      (is (= 5 (sp/count dataframe)))

      ;; Round-trip test: create, collect, and verify data integrity
      (let [collected-data (sp/collect dataframe)]
        (is (= 5 (count collected-data)))
        (is (every? map? collected-data))

        ;; Verify all required fields are present
        (doseq [row collected-data]
          (is (contains? row :id))
          (is (contains? row :name))
          (is (contains? row :age))
          (is (contains? row :department))
          (is (contains? row :salary)))))))

;; Data Type Conversion Tests
(deftest test-data-type-conversions
  (testing "Various data type conversions work correctly"
    (let [mixed-data [{:id 1 
                       :name "Test" 
                       :active true 
                       :score 95.5 
                       :created_at (java.sql.Timestamp. (System/currentTimeMillis))}]
          dataframe (sp/create-dataframe *session* mixed-data)]
      
      (is (some? dataframe))
      (let [collected (sp/collect dataframe)]
        (is (= 1 (count collected)))
        (let [row (first collected)]
          (is (integer? (:id row)))
          (is (string? (:name row)))
          (is (boolean? (:active row)))
          (is (number? (:score row))))))))

;; Feature 5 Integration Tests (Map-like Column Access)
(deftest test-feature-5-map-like-column-access
  (testing "Feature 5: Map-like access to columns"
    ;; First create and save test data
    (let [dataframe (sp/create-dataframe *session* test-data)]
      (sp/save-as-table dataframe test-table-name :overwrite)
      
      ;; Read the table back to get a fresh dataframe for testing
      (let [table-df (sp/table *session* test-table-name)]
        
        (testing "IFn access: (df :column)"
          ;; Test accessing columns using dataframe as a function
          (let [name-col (table-df :name)
                salary-col (table-df :salary)
                age-col (table-df :age)]
            ;; Verify we get Column objects back
            (is (instance? com.snowflake.snowpark_java.Column name-col))
            (is (instance? com.snowflake.snowpark_java.Column salary-col))
            (is (instance? com.snowflake.snowpark_java.Column age-col))
            
            ;; Test that we can use these columns in queries
            (let [filtered-df (sp/filter table-df (sp/gt salary-col (sp/lit 65000)))
                  results (sp/collect filtered-df)]
              (is (= 2 (count results)))  ; Alice and Bob have salary > 65000
              (is (every? #(> (:salary %) 65000) results)))))
        
        (testing "ILookup access: (:column df)"
          ;; Test accessing columns using keyword lookup
          (let [name-col (:name table-df)
                department-col (:department table-df)
                id-col (:id table-df)]
            ;; Verify we get Column objects back
            (is (instance? com.snowflake.snowpark_java.Column name-col))
            (is (instance? com.snowflake.snowpark_java.Column department-col))
            (is (instance? com.snowflake.snowpark_java.Column id-col))
            
            ;; Test that we can use these columns in queries
            (let [selected-df (sp/select table-df [name-col department-col])
                  results (sp/collect selected-df)]
              (is (= 3 (count results)))
              (is (every? #(contains? % :name) results))
              (is (every? #(contains? % :department) results))
              (is (not-any? #(contains? % :salary) results)))))  ; salary should not be in results
        
        (testing "Non-existent column returns nil"
          ;; Test both access patterns return nil for non-existent columns
          (is (nil? (table-df :non-existent-column)))
          (is (nil? (:also-non-existent table-df)))
          (is (nil? (table-df :invalid-field)))
          (is (nil? (:missing-column table-df))))
        
        (testing "Mixed column access patterns work together"
          ;; Test combining both access patterns in the same query
          (let [name-col (table-df :name)        ; IFn access
                salary-col (:salary table-df)    ; ILookup access
                condition (sp/eq salary-col (sp/lit 70000))  ; Equal to 70000, not greater than
                selected-df (sp/select table-df [name-col salary-col])
                filtered-df (sp/filter selected-df condition)
                results (sp/collect filtered-df)]
            (is (= 1 (count results)))  ; Only Alice has salary = 70000
            (let [alice (first results)]
              (is (= "Alice" (:name alice)))
              (is (= 70000 (:salary alice))))))
        
        (testing "Column access with case transformation"
          ;; Test that column names are properly transformed (lowercase keywords to uppercase strings)
          (let [mixed-case-col (table-df :DePaRtMeNt)  ; Should work regardless of case in keyword
                upper-col (:SALARY table-df)
                lower-col (:name table-df)]
            ;; All should return Column objects since the dataframe uses case-insensitive transforms
            (is (instance? com.snowflake.snowpark_java.Column mixed-case-col))
            (is (instance? com.snowflake.snowpark_java.Column upper-col))
            (is (instance? com.snowflake.snowpark_java.Column lower-col))
            
            ;; Test in a real query
            (let [selected-df (sp/select table-df [lower-col upper-col])
                  results (sp/collect selected-df)]
              (is (= 3 (count results)))
              (is (every? #(contains? % :name) results))
              (is (every? #(contains? % :salary) results)))))))))

;; Performance and Scalability Tests
(deftest test-performance-scalability
  (testing "Library handles larger datasets efficiently"
    (let [large-data (vec (repeatedly 1000 
                                      #(hash-map :id (rand-int 10000)
                                                 :name (str "User" (rand-int 1000))
                                                 :score (rand-int 100))))
          dataframe (sp/create-dataframe *session* large-data)]
      
      (is (some? dataframe))
      (is (= 1000 (sp/count dataframe)))
      
      ;; Test filtering and aggregation on larger dataset
      (let [score-col (sp/col dataframe :score)
            score-condition (sp/gt score-col (sp/lit 50))
            filtered-df (sp/filter dataframe score-condition)
            sample-data (sp/take filtered-df 10)]
        (is (<= (count sample-data) 10))
        (is (every? #(> (:score %) 50) sample-data))))))
