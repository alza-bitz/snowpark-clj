(ns snowpark-clj.dataframe-integration-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [malli.generator :as mg]
   [snowpark-clj.dataframe :as df]
   [snowpark-clj.functions :as fn]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.schemas :as schemas]
   [snowpark-clj.session :as session]
   [snowpark-clj.wrapper :as wrapper]))

;; Test configuration
(def test-config-file "integration/snowflake.edn")

;; Test fixtures
(def ^:dynamic *test-session* nil)
(def test-table-name "SNOWPARK_CLJ_TEST_DATAFRAME_TABLE")

(defn session-fixture
  "Creates a session before each test and ensures cleanup after"
  [test-fn]
  (with-open [session (session/create-session test-config-file)]
    (binding [*test-session* session]
      (try
        (test-fn)
        (finally
          ;; Clean up any test tables created
          (try
            (.sql (wrapper/unwrap session) (str "DROP TABLE IF EXISTS " test-table-name))
            (catch Exception _
              ;; Ignore errors during cleanup
              nil)))))))

(use-fixtures :each session-fixture)

(deftest test-create-dataframe
  (testing "Create dataframe from a collection of maps without schema"
    (let [data (mg/generate [:vector {:gen/min 3 :gen/max 5} schemas/employee-schema])
          result (df/create-dataframe *test-session* data)]

      ;; Verify the result is properly wrapped
      (is (wrapper/wrapper? result))
      (is (= ((wrapper/unwrap-option result :read-key-fn) "TEST_COLUMN") :test_column))
      (is (= ((wrapper/unwrap-option result :write-key-fn) :test_column) "TEST_COLUMN"))      ;; Verify we can get basic info from the dataframe
      (let [schema (df/schema result)]
        (is (some? schema))
        ;; Schema should have the expected fields
        (let [field-names (set (.names schema))]
          (is (contains? field-names "ID"))
          (is (contains? field-names "NAME"))
          (is (contains? field-names "DEPARTMENT"))
          (is (contains? field-names "SALARY"))))

      ;; Verify we can collect the data back
      (let [collected (df/collect result)]
        (is (= (count data) (count collected)))
        ;; Check that keys are properly transformed by read-key-fn
        (is (every? keyword? (mapcat keys collected)))
        ;; Check that we have the expected keys
        (is (every? #(contains? % :id) collected))
        (is (every? #(contains? % :name) collected))
        (is (every? #(contains? % :department) collected))
        (is (every? #(contains? % :salary) collected)))))

  (testing "Create dataframe from a collection of maps with explicit schema"
    (let [data (mg/generate [:vector {:gen/min 2 :gen/max 5} schemas/employee-schema-with-optional-keys])
          ;; Create a schema using the schema namespace
          session-schema (schema/malli-schema->snowpark-schema *test-session* schemas/employee-schema-with-optional-keys)
          result (df/create-dataframe *test-session* data session-schema)]

      ;; Verify the result is properly wrapped
      (is (wrapper/wrapper? result))
      (is (= ((wrapper/unwrap-option result :read-key-fn) "TEST_COLUMN") :test_column))
      (is (= ((wrapper/unwrap-option result :write-key-fn) :test_column) "TEST_COLUMN"))

      ;; Verify the schema matches what we provided
      (let [result-schema (df/schema result)]
        (is (some? result-schema))
        (let [field-names (set (.names result-schema))]
          (is (contains? field-names "ID"))
          (is (contains? field-names "NAME"))
          (is (contains? field-names "DEPARTMENT"))
          (is (contains? field-names "SALARY"))
          (is (contains? field-names "AGE"))))

      ;; Verify we can collect the data back
      (let [collected (df/collect result)]
        (is (= (count data) (count collected)))
        ;; Check that keys are properly transformed by read-key-fn
        (is (every? keyword? (mapcat keys collected))))))

  (testing "Create dataframe from empty data should throw exception"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot create dataframe from empty data"
                          (df/create-dataframe *test-session* []))))

  (testing "Create dataframe and save to table for round-trip verification"
    (let [data (mg/generate [:vector {:gen/min 3 :gen/max 3} schemas/employee-schema])
          result (df/create-dataframe *test-session* data)]

      ;; Save the dataframe to a table
      (df/save-as-table result test-table-name :overwrite)

      ;; Read it back using table function and verify
      (let [table-df (df/table *test-session* test-table-name)
            collected-data (df/collect table-df)]

        (is (= (count data) (count collected-data)))
        ;; Verify that the round-trip preserves the core data
        ;; Note: we compare the original data with collected data by converting both to comparable format
        (let [original-sorted (sort-by :id data)
              collected-sorted (sort-by :id collected-data)]
          (doseq [[orig coll] (map vector original-sorted collected-sorted)]
            (is (= (:id orig) (:id coll)))
            (is (= (:name orig) (:name coll)))
            (is (= (:department orig) (:department coll)))
            (is (= (:salary orig) (:salary coll)))))))))

(deftest test-table
  (testing "Create dataframe from existing Snowflake table"
    ;; First create some test data in a table
    (let [test-data (mg/generate [:vector {:gen/min 2 :gen/max 4} schemas/employee-schema])
          temp-df (df/create-dataframe *test-session* test-data)]

      ;; Save to table
      (df/save-as-table temp-df test-table-name :overwrite)

      ;; Now test the table function
      (let [result (df/table *test-session* test-table-name)]

        ;; Verify the result is properly wrapped
        (is (wrapper/wrapper? result))
        (is (= ((wrapper/unwrap-option result :read-key-fn) "TEST_COLUMN") :test_column))
        (is (= ((wrapper/unwrap-option result :write-key-fn) :test_column) "TEST_COLUMN"))

        ;; Verify we can get schema info
        (let [schema (df/schema result)]
          (is (some? schema))
          (let [field-names (set (.names schema))]
            (is (contains? field-names "ID"))
            (is (contains? field-names "NAME"))
            (is (contains? field-names "DEPARTMENT"))
            (is (contains? field-names "SALARY"))))

        ;; Verify we can collect the data
        (let [collected (df/collect result)]
          (is (= (count test-data) (count collected)))
          ;; Check that keys are properly transformed by read-key-fn
          (is (every? keyword? (mapcat keys collected)))
          ;; Check data integrity by comparing sorted results
          (let [original-sorted (sort-by :id test-data)
                collected-sorted (sort-by :id collected)]
            (doseq [[orig coll] (map vector original-sorted collected-sorted)]
              (is (= (:id orig) (:id coll)))
              (is (= (:name orig) (:name coll)))
              (is (= (:department orig) (:department coll)))
              (is (= (:salary orig) (:salary coll)))))))))

  (testing "Table function with non-existent table should throw exception"
    (is (thrown? Exception
                 (-> (df/table *test-session* "NON_EXISTENT_TABLE_12345")
                     (df/collect))))))

(deftest test-select
  (testing "Select specific columns from dataframe"
    ;; Create test data and save to table
    (let [test-data (mg/generate [:vector {:gen/min 3 :gen/max 5} schemas/employee-schema])
          temp-df (df/create-dataframe *test-session* test-data)]

      (testing "Select subset of columns"
        (let [result (df/select temp-df [:name :salary])
              collected (df/collect result)]

          ;; Verify we have the same number of rows
          (is (= (count test-data) (count collected)))

          ;; Verify we only have the selected columns
          (is (every? #(= #{:name :salary} (set (keys %))) collected))

          ;; Verify data integrity for selected columns
          (let [original-by-name (group-by :name test-data)
                collected-by-name (group-by :name collected)]
            (doseq [name (keys original-by-name)]
              (let [orig (first (original-by-name name))
                    coll (first (collected-by-name name))]
                (is (= (:name orig) (:name coll)))
                (is (= (:salary orig) (:salary coll))))))))

      (testing "Select single column"
        (let [result (df/select temp-df :name)
              collected (df/collect result)]

          ;; Verify we have the same number of rows
          (is (= (count test-data) (count collected)))

          ;; Verify we only have the name column
          (is (every? #(= #{:name} (set (keys %))) collected))

          ;; Verify all names are present
          (let [original-names (set (map :name test-data))
                collected-names (set (map :name collected))]
            (is (= original-names collected-names))))))))

(deftest test-df-filter
  (testing "Filter dataframe rows based on conditions"
    ;; Create test data with known values for predictable filtering
    (let [test-data [{:id 1 :name "Alice" :department "Engineering" :salary 75000}
                     {:id 2 :name "Bob" :department "Engineering" :salary 85000}
                     {:id 3 :name "Charlie" :department "Marketing" :salary 60000}
                     {:id 4 :name "Diana" :department "Sales" :salary 70000}]
          temp-df (df/create-dataframe *test-session* test-data)]

      (testing "Filter using column expression"
        ;; Filter for high salaries (> 70000)
        (let [salary-col (df/col temp-df :salary)
              gt-70k (fn/gt salary-col (fn/lit 70000))
              result (df/df-filter temp-df gt-70k)
              collected (df/collect result)]

          ;; Should get Alice and Bob (2 people) - Diana has exactly 70000, not > 70000
          (is (= 2 (count collected)))

          ;; Verify all collected salaries are > 70000
          (is (every? #(> (:salary %) 70000) collected))

          ;; Verify specific people are included
          (let [names (set (map :name collected))]
            (is (contains? names "Alice"))
            (is (contains? names "Bob"))
            (is (not (contains? names "Charlie")))
            (is (not (contains? names "Diana"))))))

      (testing "Filter using keyword column name"
        ;; Create a simple filter condition first
        (let [salary-col (df/col temp-df :salary)
              eq-75k (fn/eq salary-col (fn/lit 75000))
              result (df/df-filter temp-df eq-75k)
              collected (df/collect result)]

          ;; Should get only Alice
          (is (= 1 (count collected)))
          (is (= "Alice" (:name (first collected))))
          (is (= 75000 (:salary (first collected)))))))))

(deftest test-limit
  (testing "Limit number of rows returned from dataframe"
    ;; Create test data with enough rows to test limiting
    (let [test-data (mg/generate [:vector {:gen/min 5 :gen/max 8} schemas/employee-schema])
          temp-df (df/create-dataframe *test-session* test-data)]

      (testing "Limit to specific number of rows"
        (let [limit-count 3
              result (df/limit temp-df limit-count)
              collected (df/collect result)]

          ;; Verify we get exactly the limited number of rows
          (is (= limit-count (count collected)))

          ;; Verify the data structure is correct
          (is (every? keyword? (mapcat keys collected)))
          (is (every? #(contains? % :id) collected))
          (is (every? #(contains? % :name) collected))))

      (testing "Limit to more rows than available"
        (let [limit-count (+ (count test-data) 5)
              result (df/limit temp-df limit-count)
              collected (df/collect result)]

          ;; Should return all available rows
          (is (= (count test-data) (count collected)))))

      (testing "Limit to zero rows"
        (let [result (df/limit temp-df 0)
              collected (df/collect result)]

          ;; Should return empty collection
          (is (= 0 (count collected))))))))

(deftest test-df-sort
  (testing "Sort dataframe by columns"
    ;; Create test data with known values for predictable sorting
    (let [test-data [{:id 3 :name "Charlie" :department "Engineering" :salary 80000}
                     {:id 1 :name "Alice" :department "Marketing" :salary 75000}
                     {:id 4 :name "Diana" :department "Sales" :salary 70000}
                     {:id 2 :name "Bob" :department "Engineering" :salary 85000}]
          temp-df (df/create-dataframe *test-session* test-data)]

      (testing "Sort by single column (ascending by default)"
        (let [result (df/df-sort temp-df :id)
              collected (df/collect result)]

          ;; Verify all rows are present
          (is (= (count test-data) (count collected)))

          ;; Verify sorting by ID (should be 1, 2, 3, 4)
          (is (= [1 2 3 4] (map :id collected)))

          ;; Verify corresponding names
          (is (= ["Alice" "Bob" "Charlie" "Diana"] (map :name collected)))))

      (testing "Sort by multiple columns"
        (let [result (df/df-sort temp-df [:department :salary])
              collected (df/collect result)]

          ;; Verify all rows are present
          (is (= (count test-data) (count collected)))

          ;; Verify department sorting (Engineering, Marketing, Sales)
          ;; Within Engineering, salary should be sorted (80000, 85000)
          (let [departments (map :department collected)]
            (is (= "Engineering" (first departments)))
            (is (= "Engineering" (second departments)))
            (is (= "Marketing" (nth departments 2)))
            (is (= "Sales" (last departments))))

          ;; Verify within Engineering dept, Charlie (80k) comes before Bob (85k)
          (let [engineering-rows (filter #(= "Engineering" (:department %)) collected)]
            (is (= "Charlie" (:name (first engineering-rows))))
            (is (= "Bob" (:name (second engineering-rows)))))))

      (testing "Sort by name (string column)"
        (let [result (df/df-sort temp-df :name)
              collected (df/collect result)]

          ;; Verify all rows are present
          (is (= (count test-data) (count collected)))

          ;; Verify alphabetical sorting by name
          (is (= ["Alice" "Bob" "Charlie" "Diana"] (map :name collected))))))))

(deftest test-collect
  (testing "Collect dataframe rows to local collection"
    ;; Create test data with known values for predictable results
    (let [test-data [{:id 1 :name "Alice" :department "Engineering" :salary 75000}
                     {:id 2 :name "Bob" :department "Marketing" :salary 80000}
                     {:id 3 :name "Charlie" :department "Sales" :salary 65000}]
          dataframe (df/create-dataframe *test-session* test-data)]
      
      (testing "Basic collect functionality"
        (let [collected (df/collect dataframe)]
          
          ;; Verify we get all rows
          (is (= (count test-data) (count collected)))
          
          ;; Verify all collected items are maps with keyword keys
          (is (every? map? collected))
          (is (every? #(every? keyword? (keys %)) collected))
          
          ;; Verify expected keys are present
          (is (every? #(contains? % :id) collected))
          (is (every? #(contains? % :name) collected))
          (is (every? #(contains? % :department) collected))
          (is (every? #(contains? % :salary) collected))))
      
      (testing "Collect preserves data integrity"
        (let [collected (df/collect dataframe)
              ;; Sort both collections by id for comparison
              original-sorted (sort-by :id test-data)
              collected-sorted (sort-by :id collected)]
          
          ;; Verify each row matches the original
          (doseq [[orig coll] (map vector original-sorted collected-sorted)]
            (is (= (:id orig) (:id coll)))
            (is (= (:name orig) (:name coll)))
            (is (= (:department orig) (:department coll)))
            (is (= (:salary orig) (:salary coll))))))
      
      (testing "Collect works with transformations"
        ;; Apply some transformations and collect
        (let [filtered-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 70000)))
              collected (df/collect filtered-df)]
          
          ;; Should only get Alice and Bob (salary > 70000)
          (is (= 2 (count collected)))
          (is (every? #(> (:salary %) 70000) collected))
          
          ;; Verify the specific people
          (let [names (set (map :name collected))]
            (is (contains? names "Alice"))
            (is (contains? names "Bob"))
            (is (not (contains? names "Charlie"))))))
      
      (testing "Collect on empty result"
        (let [empty-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 100000)))
              collected (df/collect empty-df)]
          
          ;; Should return empty collection
          (is (empty? collected))
          (is (= 0 (count collected))))))))

(deftest test-show
  (testing "Show dataframe content for debugging/inspection"
    ;; Create test data with known values for predictable results
    (let [test-data [{:id 1 :name "Alice" :department "Engineering" :salary 75000}
                     {:id 2 :name "Bob" :department "Marketing" :salary 80000}
                     {:id 3 :name "Charlie" :department "Sales" :salary 65000}]
          dataframe (df/create-dataframe *test-session* test-data)]
      
      (testing "Basic show functionality"
        ;; show returns nil but prints output - we can test it doesn't throw
        (is (nil? (df/show dataframe))))
      
      (testing "Show with limited rows"
        ;; Test with explicit row limit
        (is (nil? (df/show dataframe 2))))
      
      (testing "Show with very large limit"
        ;; Should handle limits larger than available data
        (is (nil? (df/show dataframe 100))))
      
      (testing "Show on empty dataframe"
        ;; Create an empty result and show it
        (let [empty-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 100000)))]
          (is (nil? (df/show empty-df)))))
      
      (testing "Show works with transformations"
        ;; Apply transformations and show the result
        (let [filtered-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 70000)))
              limited-df (df/limit filtered-df 1)]
          (is (nil? (df/show limited-df))))))))

(deftest test-df-count
  (testing "Count rows in dataframe"
    ;; Create test data with known number of rows
    (let [test-data [{:id 1 :name "Alice" :department "Engineering" :salary 75000}
                     {:id 2 :name "Bob" :department "Marketing" :salary 80000}
                     {:id 3 :name "Charlie" :department "Sales" :salary 65000}
                     {:id 4 :name "Diana" :department "HR" :salary 55000}
                     {:id 5 :name "Eve" :department "Engineering" :salary 90000}]
          dataframe (df/create-dataframe *test-session* test-data)]
      
      (testing "Basic count functionality"
        (let [count-result (df/df-count dataframe)]
          ;; df-count should return the number of rows
          (is (= (count test-data) count-result))
          (is (= 5 count-result))))
      
      (testing "Count on empty dataframe"
        ;; Create an empty result by filtering with impossible condition
        (let [empty-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 100000)))
              count-result (df/df-count empty-df)]
          
          ;; Should return 0 for empty dataframe
          (is (= 0 count-result))))
      
      (testing "Count works with transformations"
        ;; Apply filter transformation and count
        (let [filtered-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 70000)))
              count-result (df/df-count filtered-df)]
          
          ;; Should count only rows where salary > 70000 (Alice, Bob, Eve = 3 people)
          (is (= 3 count-result))))
      
      (testing "Count after select transformation"
        ;; Select specific columns and count - should still return same number of rows
        (let [selected-df (df/select dataframe [:name :department])
              count-result (df/df-count selected-df)]
          
          ;; Should return same count as original dataframe
          (is (= (count test-data) count-result))
          (is (= 5 count-result))))
      
      (testing "Count after limit transformation"
        ;; Apply limit and count
        (let [limited-df (df/limit dataframe 3)
              count-result (df/df-count limited-df)]
          
          ;; Should return the limited number of rows
          (is (= 3 count-result))))
      
      (testing "Count is eager operation"
        ;; Verify that count actually executes and doesn't just return a lazy transformation
        ;; This is tested implicitly by the fact that we get actual numbers, not wrapper objects
        (let [count-result (df/df-count dataframe)]
          (is (number? count-result))
          (is (pos? count-result)))))))

(deftest test-df-take
  (testing "Take first N rows from dataframe"
    ;; Create test data with known values for predictable results
    (let [test-data [{:id 1 :name "Alice" :department "Engineering" :salary 75000}
                     {:id 2 :name "Bob" :department "Marketing" :salary 80000}
                     {:id 3 :name "Charlie" :department "Sales" :salary 65000}
                     {:id 4 :name "Diana" :department "HR" :salary 55000}
                     {:id 5 :name "Eve" :department "Engineering" :salary 90000}]
          dataframe (df/create-dataframe *test-session* test-data)]

      (testing "Basic take functionality"
        (let [taken-rows (df/df-take dataframe 3)]

          ;; Should return exactly 3 rows
          (is (= 3 (count taken-rows)))

          ;; Should return collection of maps with keyword keys
          (is (every? map? taken-rows))
          (is (every? #(every? keyword? (keys %)) taken-rows))

          ;; Should have expected keys
          (is (every? #(contains? % :id) taken-rows))
          (is (every? #(contains? % :name) taken-rows))
          (is (every? #(contains? % :department) taken-rows))
          (is (every? #(contains? % :salary) taken-rows))))

      (testing "Take more rows than available"
        (let [taken-rows (df/df-take dataframe 10)]

          ;; Should return all available rows (5 in this case)
          (is (= (count test-data) (count taken-rows)))
          (is (= 5 (count taken-rows)))))

      (testing "Take zero rows"
        (let [taken-rows (df/df-take dataframe 0)]

          ;; Should return empty collection
          (is (empty? taken-rows))
          (is (= 0 (count taken-rows)))))

      (testing "Take from empty dataframe"
        ;; Create an empty result by filtering with impossible condition
        (let [empty-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 100000)))
              taken-rows (df/df-take empty-df 5)]

          ;; Should return empty collection
          (is (empty? taken-rows))
          (is (= 0 (count taken-rows)))))

      (testing "Take works with transformations"
        ;; Apply filter transformation and take
        (let [filtered-df (df/df-filter dataframe (fn/gt (df/col dataframe :salary) (fn/lit 70000)))
              taken-rows (df/df-take filtered-df 2)]

          ;; Should take 2 rows from filtered result (high salary employees)
          (is (= 2 (count taken-rows)))
          (is (every? #(> (:salary %) 70000) taken-rows))))

      (testing "Take preserves data integrity"
        (let [taken-rows (df/df-take dataframe 2)]

          ;; Should preserve the structure and data of the first 2 rows
          ;; Note: Order might vary due to Snowflake's processing, so we check data integrity differently
          (is (= 2 (count taken-rows)))

          ;; Verify that all taken rows exist in original data
          (let [original-ids (set (map :id test-data))
                taken-ids (set (map :id taken-rows))]
            (is (every? #(contains? original-ids %) taken-ids)))))

      (testing "Take is eager operation"
        ;; Verify that take actually executes and returns data, not a lazy transformation
        (let [taken-rows (df/df-take dataframe 1)]
          (is (coll? taken-rows))
          (is (every? map? taken-rows))
          ;; Should be a realized collection, not a dataframe wrapper
          (is (not (wrapper/wrapper? taken-rows)))))

      (testing "Take single row"
        (let [taken-rows (df/df-take dataframe 1)]

          ;; Should return exactly 1 row
          (is (= 1 (count taken-rows)))

          ;; Verify it's properly structured
          (let [single-row (first taken-rows)]
            (is (map? single-row))
            (is (every? keyword? (keys single-row)))
            (is (contains? single-row :id))
            (is (contains? single-row :name))
            (is (contains? single-row :department))
            (is (contains? single-row :salary))))))))

