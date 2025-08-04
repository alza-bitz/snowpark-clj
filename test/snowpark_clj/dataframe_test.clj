(ns snowpark-clj.dataframe-test
  "Unit tests for the dataframe namespace"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [snowpark-clj.dataframe :as df]
            [snowpark-clj.convert :as convert]
            [snowpark-clj.session :as session]
            [malli.core :as m]
            [spy.core :as spy]
            [spy.assert :as assert]))

;; Test data
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

(deftest test-create-dataframe
  (testing "Creating DataFrame from Clojure data (2-arity)"
    ;; Mock the Snowpark API calls that require connection
    (let [mock-session (reify Object (toString [_] "mock-session"))
          mock-dataframe (reify Object (toString [_] "mock-dataframe"))
          session-wrapper {:session mock-session :read-key-fn keyword :write-key-fn name}
          create-spy (spy/spy (constantly mock-dataframe))]
      (with-redefs [session/unwrap-session (constantly mock-session)
                    session/get-write-key-fn (constantly name)
                    convert/infer-schema (fn [_data _write-key-fn] (reify Object))
                    convert/maps->rows (fn [_data _schema _write-key-fn] [])]
                    
        (with-redefs-fn {#'df/create-dataframe 
                         (fn 
                           ([session data]
                            (df/create-dataframe session data (convert/infer-schema data (comp str/upper-case name))))
                           ([session data schema]
                            (create-spy session data schema)
                            (df/wrap-dataframe mock-dataframe session)))}
          #(let [result (df/create-dataframe session-wrapper test-data)]
             (is (= mock-dataframe (:dataframe result)))
             (is (= keyword (:read-key-fn result)))
             (is (= name (:write-key-fn result)))
             (assert/called-once? create-spy)))))))

(deftest test-create-dataframe-empty-data
  (testing "Creating DataFrame from empty data should throw exception"
    (is (thrown-with-msg? IllegalArgumentException 
                          #"Cannot create DataFrame from empty data"
                          (df/create-dataframe {:session nil :key-fn identity} [])))))

(deftest test-create-dataframe-with-snowpark-schema
  (testing "Creating DataFrame from Clojure data with explicit Snowpark schema (3-arity)"
    ;; Mock the Snowpark API calls and test 3-arity version
    (let [mock-session (reify Object (toString [_] "mock-session"))
          mock-dataframe (reify Object (toString [_] "mock-dataframe"))
          snowpark-schema (convert/malli-schema->snowpark-schema test-employee-schema (comp str/upper-case name))
          create-spy (spy/spy (constantly mock-dataframe))]
      (with-redefs [session/unwrap-session (constantly mock-session)
                    df/create-dataframe (fn [session data schema]
                                          (create-spy session data schema)
                                          mock-dataframe)]
        (let [result (df/create-dataframe {:session mock-session} test-data snowpark-schema)]
          (is (= mock-dataframe result))
          (assert/called-once? create-spy))))))

(deftest test-table
  (testing "Table function calls session.table with correct parameters"
    (let [mock-session (reify Object (toString [_] "mock-snowpark-session"))
          table-name "test_table"
          mock-dataframe (reify Object (toString [_] "mock-dataframe"))
          table-spy (spy/spy (constantly mock-dataframe))]
      (with-redefs [session/unwrap-session (constantly mock-session)
                    df/table (fn [session name]
                               (table-spy session name)
                               mock-dataframe)]
        (let [result (df/table {:session mock-session} table-name)]
          (is (= mock-dataframe result))
          (assert/called-once? table-spy))))))

(deftest test-select
  (testing "Select function converts columns and calls DataFrame.select"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          columns [:name :salary]
          mock-result (reify Object (toString [_] "selected-dataframe"))
          select-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/select (fn [df cols]
                                (select-spy df cols)
                                mock-result)]
        (let [result (df/select mock-df columns)]
          (is (= mock-result result))
          (assert/called-once? select-spy))))))

(deftest test-df-filter
  (testing "Filter function calls DataFrame.filter with condition"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          condition "salary > 50000"
          mock-result (reify Object (toString [_] "filtered-dataframe"))
          filter-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/df-filter (fn [df cond]
                                   (filter-spy df cond)
                                   mock-result)]
        (let [result (df/df-filter mock-df condition)]
          (is (= mock-result result))
          (assert/called-once? filter-spy))))))

(deftest test-limit
  (testing "Limit function calls DataFrame.limit with correct parameter"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          n 10
          mock-result (reify Object (toString [_] "limited-dataframe"))
          limit-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/limit (fn [df num]
                               (limit-spy df num)
                               mock-result)]
        (let [result (df/limit mock-df n)]
          (is (= mock-result result))
          (assert/called-once? limit-spy))))))

(deftest test-df-sort
  (testing "Sort function handles single and multiple columns"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          single-col :salary
          multi-cols [:salary :name]
          mock-result (reify Object (toString [_] "sorted-dataframe"))
          sort-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/df-sort (fn [df cols]
                                 (sort-spy df cols)
                                 mock-result)]
        ;; Test single column
        (let [result1 (df/df-sort mock-df single-col)]
          (is (= mock-result result1)))
        
        ;; Test multiple columns
        (let [result2 (df/df-sort mock-df multi-cols)]
          (is (= mock-result result2)))
        
        (assert/called-n-times? sort-spy 2)))))

(deftest test-df-group-by
  (testing "Group-by function converts column names and calls DataFrame.groupBy"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          columns [:department :age]
          mock-result (reify Object (toString [_] "grouped-dataframe"))
          group-by-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/df-group-by (fn [df cols]
                                     (group-by-spy df cols)
                                     mock-result)]
        (let [result (df/df-group-by mock-df columns)]
          (is (= mock-result result))
          (assert/called-once? group-by-spy))))))

(deftest test-join
  (testing "Join function handles different join types"
    (let [left-df (reify Object (toString [_] "left-df"))
          right-df (reify Object (toString [_] "right-df"))
          join-expr "left.id = right.id"
          mock-result (reify Object (toString [_] "joined-dataframe"))
          join-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/join (fn [left right expr & [type]]
                              (join-spy left right expr type)
                              mock-result)]
        ;; Test default join type (2-arity)
        (let [result1 (df/join left-df right-df join-expr)]
          (is (= mock-result result1)))
        
        ;; Test explicit join type (3-arity)
        (let [result2 (df/join left-df right-df join-expr :left)]
          (is (= mock-result result2)))
        
        (assert/called-n-times? join-spy 2)))))

(deftest test-collect
  (testing "Collect function converts DataFrame to maps with proper key-fn"
    (let [mock-df {:dataframe (reify Object (toString [_] "mock-dataframe"))
                   :read-key-fn keyword}
          expected-data [{:ID 1 :NAME "Alice"} {:ID 2 :NAME "Bob"}]
          collect-spy (spy/spy (constantly expected-data))]
      (with-redefs [df/collect (fn [df]
                                 (collect-spy df)
                                 expected-data)]
        ;; Test collect function (takes only df)
        (let [result (df/collect mock-df)]
          (is (= expected-data result)))
        
        (assert/called-once? collect-spy)))))

(deftest test-show
  (testing "Show function calls DataFrame.show with correct parameters"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          show-spy (spy/spy (constantly nil))]
      (with-redefs [df/show (fn [df & [n]]
                              (show-spy df n)
                              nil)]
        ;; Test with default row count (1-arity)
        (df/show mock-df)
        
        ;; Test with explicit row count (2-arity)
        (df/show mock-df 5)
        
        (assert/called-n-times? show-spy 2)))))

(deftest test-df-count
  (testing "Count function calls DataFrame.count"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          expected-count 42
          count-spy (spy/spy (constantly expected-count))]
      (with-redefs [df/df-count (fn [df]
                                  (count-spy df)
                                  expected-count)]
        (let [result (df/df-count mock-df)]
          (is (= expected-count result))
          (assert/called-once? count-spy))))))

(deftest test-df-take
  (testing "Take function handles correct arity"
    (let [mock-df {:dataframe (reify Object (toString [_] "mock-dataframe"))
                   :read-key-fn keyword}
          expected-data [{:ID 1 :NAME "Alice"}]
          take-spy (spy/spy (constantly expected-data))]
      (with-redefs [df/df-take (fn [df n]
                                 (take-spy df n)
                                 expected-data)]
        ;; Test df-take with correct arity (takes df and n)
        (let [result (df/df-take mock-df 1)]
          (is (= expected-data result)))
        
        (assert/called-once? take-spy)))))

(deftest test-first-row
  (testing "First-row function gets first row"
    (let [mock-df {:dataframe (reify Object (toString [_] "mock-dataframe"))
                   :read-key-fn keyword}
          expected-row {:ID 1 :NAME "Alice"}
          first-row-spy (spy/spy (constantly expected-row))]
      (with-redefs [df/first-row (fn [df]
                                   (first-row-spy df)
                                   expected-row)]
        ;; Test first-row with correct arity (takes only df)
        (let [result (df/first-row mock-df)]
          (is (= expected-row result)))
        
        (assert/called-once? first-row-spy)))))

(deftest test-write
  (testing "Write function returns DataFrameWriter"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          mock-writer (reify Object (toString [_] "mock-writer"))
          write-spy (spy/spy (constantly mock-writer))]
      (with-redefs [df/write (fn [df]
                               (write-spy df)
                               mock-writer)]
        (let [result (df/write mock-df)]
          (is (= mock-writer result))
          (assert/called-once? write-spy))))))

(deftest test-save-as-table
  (testing "Save-as-table function handles different modes and options"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          table-name "test_table"
          mock-result (reify Object (toString [_] "saved"))
          save-spy (spy/spy (constantly mock-result))]
      (with-redefs [df/save-as-table (fn [df name & [mode options]]
                                       (save-spy df name mode options)
                                       mock-result)]
        ;; Test simple save (2-arity)
        (let [result1 (df/save-as-table mock-df table-name)]
          (is (= mock-result result1)))
        
        ;; Test with mode (3-arity)
        (let [result2 (df/save-as-table mock-df table-name :overwrite)]
          (is (= mock-result result2)))
        
        ;; Test with mode and options (4-arity)
        (let [result3 (df/save-as-table mock-df table-name :overwrite {:cluster-by ["col1"]})]
          (is (= mock-result result3)))
        
        (assert/called-n-times? save-spy 3)))))

(deftest test-schema
  (testing "Schema function returns DataFrame schema"
    (let [mock-df (reify Object (toString [_] "mock-dataframe"))
          mock-schema (reify Object (toString [_] "mock-schema"))
          schema-spy (spy/spy (constantly mock-schema))]
      (with-redefs [df/schema (fn [df]
                                (schema-spy df)
                                mock-schema)]
        (let [result (df/schema mock-df)]
          (is (= mock-schema result))
          (assert/called-once? schema-spy))))))

; Create a protocol that matches what we need from DataFrame
(defprotocol MockDataFrame
  (col [this col-name] "Mock DataFrame.col method"))

;; Helper function to create mock DataFrame with call tracking
(defn- create-mock-dataframe []
  (let [received-calls (atom [])]
    {:received-calls received-calls
     :mock-dataframe (reify MockDataFrame
                       (col [_ col-name]
                         (swap! received-calls conj col-name)
                         (proxy [Object] [] (toString [] col-name)))
                       Object
                       (toString [_] "mock-snowpark-dataframe"))}))

(deftest test-col
  (testing "Col function returns Column object with transformed name"
    ;; Create a mock DataFrame object that has a .col method
    (let [{:keys [received-calls mock-dataframe]} (create-mock-dataframe)
          test-df {:dataframe mock-dataframe :write-key-fn (comp str/upper-case name)}]
      
      ;; Test with keyword column name
      (let [result (df/col test-df :name)]
        (is (some? result))
        (is (= "NAME" (str result)))
        ;; Verify the transformation was applied before calling .col
        (is (= ["NAME"] @received-calls)))
      
      ;; Reset and test with string column name
      (reset! received-calls [])
      (let [result (df/col test-df "department")]
        (is (some? result)) 
        (is (= "DEPARTMENT" (str result)))
        (is (= ["DEPARTMENT"] @received-calls)))))
  
  (testing "Col function works with different write-key-fn transformations"
    ;; Test different transformation functions using mock DataFrames with .col methods
    (testing "Lowercase transformation"
      (let [{:keys [received-calls mock-dataframe]} (create-mock-dataframe)
            lowercase-df {:dataframe mock-dataframe :write-key-fn (comp str/lower-case name)}
            result (df/col lowercase-df :COLUMN_NAME)]
        (is (some? result))
        (is (= "column_name" (str result)))
        (is (= ["column_name"] @received-calls))))
    
    (testing "String transformation (no case change)"
      (let [{:keys [received-calls mock-dataframe]} (create-mock-dataframe)
            identity-df {:dataframe mock-dataframe :write-key-fn str}
            result (df/col identity-df :column-name)]
        (is (some? result))
        (is (= ":column-name" (str result)))
        (is (= [":column-name"] @received-calls))))))

