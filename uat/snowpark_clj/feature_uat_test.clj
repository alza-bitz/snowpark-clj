(ns snowpark-clj.feature-uat-test
  "Integration tests for the snowpark-clj library"
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [malli.generator :as mg]
   [snowpark-clj.core :as sp]
   [snowpark-clj.fixtures :refer [*session* session-fixture]]
   [snowpark-clj.schemas :as schemas]
   [snowpark-clj.wrapper :as wrapper]
   [tablecloth.api :as tc]))

(def test-data
  [{:id 1 :name "Alice" :department "Engineering" :salary 70000}
   {:id 2 :name "Bob" :department "Engineering" :salary 80000}
   {:id 3 :name "Charlie" :department "Sales" :salary 60000}])

(def test-table-name "SNOWPARK_CLJ_TEST_FEATURE_TABLE")

(use-fixtures :each (session-fixture "integration/snowflake.edn" test-table-name))

;; Feature 1
(deftest test-feature-1-create-and-save-dataframe
  (testing "Feature 1: Load Clojure data and save to Snowflake table"
    ;; Given: a Snowpark session and vector of Clojure maps
    (let [dataframe (sp/create-dataframe *session* test-data)]

      ;; Verify DataFrame was created successfully (part of Given)
      (is (some? dataframe))
      (is (map? dataframe))
      (is (wrapper/unwrap dataframe))
      (is (wrapper/unwrap-option dataframe :col->key-fn))
      (is (wrapper/unwrap-option dataframe :key->col-fn))

      ;; Verify schema and columns (part of Given)
      (let [schema (sp/schema dataframe)]
        (is (some? schema)))

      (let [columns (.names (sp/schema dataframe))]
        (is (= 4 (count columns)))
        (is (every? #{"ID" "NAME" "DEPARTMENT" "SALARY"} columns)))

      ;; When: creating a dataframe and saving to table
      (sp/save-as-table dataframe test-table-name :overwrite)

      ;; Then: data is successfully persisted and retrievable
      (let [table-df (sp/table *session* test-table-name)
            result (sp/collect table-df)]
        (is (= 3 (count result)))
        (is (every? map? result))
        
        ;; Verify data integrity (part of Then)
        (is (= (sort-by :id test-data) (sort-by :id result)))))))

;; Feature 2
(deftest test-feature-2-read-and-compute
  (testing "Feature 2: Read from table, compute, and collect results"
    ;; Given: data saved in a Snowflake table
    (let [dataframe (sp/create-dataframe *session* test-data)]
      (sp/save-as-table dataframe test-table-name :overwrite)

      ;; When: applying transformations and collecting results
      (let [table-df (sp/table *session* test-table-name)
            results (-> table-df
                        (sp/filter (sp/gt (sp/col table-df :salary) (sp/lit 65000)))
                        (sp/select [:name :salary])
                        (sp/sort :salary)
                        (sp/limit 2)
                        (sp/collect))]

        ;; Then: computed results match expected filtered and sorted data
        (is (vector? results))
        (is (<= (count results) 2))

        ;; Verify data integrity (part of Then)
        (let [expected (transduce (comp (filter #(> (get % :salary) 65000))
                                        (map #(select-keys % [:name :salary])))
                                  (completing conj #(sort-by :salary %)) test-data)]
          (is (= expected results))))

      ;; Additional verifications (part of Then)
      (let [table-df (sp/table *session* test-table-name)
            row-count (sp/row-count table-df)]
        (is (= 3 row-count)))

      (let [table-df (sp/table *session* test-table-name)
            sample-rows (sp/take table-df 2)]
        (is (= 2 (count sample-rows)))
        (is (every? map? sample-rows))))))

;; Feature 3 (Session Macros)
(deftest test-feature-3-session-macros
  (testing "Feature 3: Session macros work correctly"
    ;; Given: a session created with with-open macro
    ;; When: performing dataframe operations
    ;; Then: session is automatically closed after execution
    (let [result (with-open [session (sp/create-session "integration/snowflake.edn")]
                   (let [df (sp/create-dataframe session test-data)]
                     (sp/row-count df)))]
      (is (= 3 result)))))

;; Feature 4 (Malli Schema Conversion)
(deftest test-feature-4-malli-schema-conversion
  (testing "Feature 4: Create Snowpark schema from Malli schema"
    ;; Given: a Malli schema and generated test data
    (let [data (mg/generate [:vector {:gen/min 5 :gen/max 5} schemas/employee-schema-with-optional-keys])
          schema (sp/malli->schema *session* schemas/employee-schema-with-optional-keys)
          ;; When: creating dataframe with schema and collecting
          dataframe (sp/create-dataframe *session* data schema)]

      (is (= 5 (sp/row-count dataframe)))

      ;; Then: round-trip data integrity is preserved
      (let [result (sp/collect dataframe)]
        (is (= 5 (count result)))
        (is (every? map? result))

        ;; Verify data integrity (part of Then)
        (is (= (sort-by :id data) (sort-by :id result)))))))

;; Feature 5 (Map-like Column Access)
(deftest test-feature-5-map-like-column-access
  (testing "Feature 5: Map-like access to columns"
    ;; First create and save test data
    (let [temp-df (sp/create-dataframe *session* test-data)]
       
      (testing "IFn access: (df :column)"
        ;; Given: a dataframe with columns
        ;; When: accessing columns using function syntax
        (let [name-col (temp-df :name)
              salary-col (temp-df :salary)]
          ;; Then: Column objects are returned for valid queries
          (is (instance? com.snowflake.snowpark_java.Column name-col))
          (is (instance? com.snowflake.snowpark_java.Column salary-col))
          
          ;; Verify we can use these columns in queries (part of Then)
          (let [filtered-df (sp/filter temp-df (sp/gt salary-col (sp/lit 65000)))
                results (sp/collect filtered-df)]
            (is (= 2 (count results)))  ; Alice and Bob have salary > 65000
            (is (every? #(> (:salary %) 65000) results)))))
      
      (testing "ILookup access: (:column df)"
        ;; Given: a dataframe with columns
        ;; When: accessing columns using keyword lookup
        (let [name-col (:name temp-df)
              department-col (:department temp-df)
              id-col (:id temp-df)]
          ;; Then: Column objects are returned for valid queries
          (is (instance? com.snowflake.snowpark_java.Column name-col))
          (is (instance? com.snowflake.snowpark_java.Column department-col))
          (is (instance? com.snowflake.snowpark_java.Column id-col))
          
          ;; Verify we can use these columns in queries (part of Then)
          (let [selected-df (sp/select temp-df [name-col department-col])
                results (sp/collect selected-df)]
            (is (= 3 (count results)))
            (is (every? #(contains? % :name) results))
            (is (every? #(contains? % :department) results))
            (is (not-any? #(contains? % :salary) results)))))  ; salary should not be in results
      
      (testing "Non-existent column returns nil"
        ;; Given: a dataframe
        ;; When: accessing non-existent columns
        ;; Then: nil is returned for both access patterns
        (is (nil? (temp-df :non-existent-column)))
        (is (nil? (:also-non-existent temp-df)))
        (is (nil? (temp-df :invalid-field)))
        (is (nil? (:missing-column temp-df))))
      
      (testing "Mixed column access patterns work together"
        ;; Given: a dataframe
        ;; When: combining IFn and ILookup access patterns
        (let [name-col (temp-df :name)        ; IFn access
              salary-col (:salary temp-df)    ; ILookup access
              condition (sp/eq salary-col (sp/lit 70000))  ; Equal to 70000, not greater than
              selected-df (sp/select temp-df [name-col salary-col])
              filtered-df (sp/filter selected-df condition)
              results (sp/collect filtered-df)]
          ;; Then: both patterns work correctly in the same query
          (is (= 1 (count results)))  ; Only Alice has salary = 70000
          (let [alice (first results)]
            (is (= "Alice" (:name alice)))
            (is (= 70000 (:salary alice))))))
      
      (testing "Column access with case transformation"
        ;; Given: a dataframe with various case column names
        ;; When: accessing columns with different cases
        (let [mixed-case-col (temp-df :DePaRtMeNt)  ; Should work regardless of case in keyword
              upper-col (:SALARY temp-df)
              lower-col (:name temp-df)]
          ;; Then: proper case transformation occurs and queries succeed
          (is (instance? com.snowflake.snowpark_java.Column mixed-case-col))
          (is (instance? com.snowflake.snowpark_java.Column upper-col))
          (is (instance? com.snowflake.snowpark_java.Column lower-col))
          
          ;; Verify in a real query (part of Then)
          (let [selected-df (sp/select temp-df [lower-col upper-col])
                results (sp/collect selected-df)]
            (is (= 3 (count results)))
            (is (every? #(contains? % :name) results))
            (is (every? #(contains? % :salary) results))))))))

;; Feature 6
(deftest test-feature-6-convert-to-tablecloth-dataset
  (testing "Feature 6: Transform to a tech.ml.dataset or Tablecloth dataset"
    ;; Given: collected Snowpark data (setup)
    (let [data (mg/generate [:vector {:gen/min 10 :gen/max 10} schemas/employee-schema-with-optional-keys])
          schema (sp/malli->schema *session* schemas/employee-schema-with-optional-keys)
          dataframe (sp/create-dataframe *session* data schema)
          collected-data (sp/collect dataframe)
          ;; When: converting to Tablecloth dataset
          tc-dataset (tc/dataset collected-data)]
      
      (testing "Tablecloth dataset creation succeeds"
        ;; Given: collected Snowpark data
        ;; When: converting to Tablecloth dataset
        ;; Then: dataset is created with correct dimensions and columns
        (is (some? tc-dataset))
        (is (= 10 (tc/row-count tc-dataset)))
        (is (= 5 (tc/column-count tc-dataset)))
        (is (= [:id :name :department :salary :age] (tc/column-names tc-dataset))))
      
      (testing "Key transformations are preserved"
        ;; Given: Snowflake uppercase column names
        ;; When: converting to Tablecloth
        ;; Then: columns are transformed to lowercase keywords
        (let [first-row (first collected-data)
              tc-columns (tc/column-names tc-dataset)]
          (is (every? keyword? (keys first-row)))
          (is (every? #(= (name %) (str/lower-case (name %))) (keys first-row)))
          (is (every? keyword? tc-columns))))
      
      (testing "Data values are correctly preserved"
        ;; Given: original and collected data
        ;; When: comparing values
        ;; Then: all data values survive the round trip correctly
        (is (= (count data) (count collected-data)))
        (is (= (count data) (tc/row-count tc-dataset)))
        
        ;; Verify all original data is present (part of Then)
        (let [original-ids (set (map :id data))
              collected-ids (set (map :id collected-data))
              tc-ids (set (tc/column tc-dataset :id))]
          (is (= original-ids collected-ids))
          (is (= original-ids tc-ids))))
      
      (testing "Tablecloth operations work correctly"
        ;; Given: a Tablecloth dataset from Snowpark
        ;; When: performing Tablecloth operations
        ;; Then: operations work correctly and can be chained with Snowpark
        
        ;; Test basic Tablecloth operations (part of Then)
        (let [info-result (tc/info tc-dataset)]
          (is (some? info-result))
          (is (= 5 (tc/row-count info-result))))  ; 5 columns described
        
        ;; Test column selection (part of Then)
        (let [selected (tc/select-columns tc-dataset [:name :department])]
          (is (= 2 (tc/column-count selected)))
          (is (= 10 (tc/row-count selected)))
          (is (= [:name :department] (tc/column-names selected))))
        
        ;; Test row filtering (part of Then)
        (let [engineering (tc/select-rows tc-dataset #(= "Engineering" (:department %)))]
          (is (every? #(= "Engineering" (:department %)) (tc/rows engineering :as-maps)))
          (is (<= (tc/row-count engineering) 10)))
        
        ;; Test aggregation (part of Then)
        (let [grouped (-> tc-dataset
                         (tc/group-by [:department])
                         (tc/aggregate {:employee-count tc/row-count}))]
          (is (some? grouped))
          (is (every? #(> (:employee-count %) 0) (tc/rows grouped :as-maps)))
          (is (every? #(contains? % :department) (tc/rows grouped :as-maps))))
        
        ;; Test chaining Snowpark and Tablecloth operations (part of Then)
        (let [final-result (-> dataframe
                              (sp/filter (sp/gt (sp/col dataframe :salary) (sp/lit 60000)))
                              (sp/collect)
                              (tc/dataset)
                              (tc/select-columns [:name :salary])
                              (tc/rows :as-maps)
                              vec)]
          (is (vector? final-result))
          (is (every? #(> (:salary %) 60000) final-result))
          (is (every? #(contains? % :name) final-result))
          (is (every? #(contains? % :salary) final-result)))))))

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

;; Performance and Scalability Tests
(deftest test-performance-scalability
  (testing "Library handles larger datasets efficiently"
    (let [large-data (vec (repeatedly 1000 
                                      #(hash-map :id (rand-int 10000)
                                                 :name (str "User" (rand-int 1000))
                                                 :score (rand-int 100))))
          dataframe (sp/create-dataframe *session* large-data)]
      
      (is (some? dataframe))
      (is (= 1000 (sp/row-count dataframe)))
      
      ;; Test filtering and aggregation on larger dataset
      (let [score-col (sp/col dataframe :score)
            score-condition (sp/gt score-col (sp/lit 50))
            filtered-df (sp/filter dataframe score-condition)
            sample-data (sp/take filtered-df 10)]
        (is (<= (count sample-data) 10))
        (is (every? #(> (:score %) 50) sample-data))))))
