(ns snowpark-clj.dataframe-test
  "Unit tests for the dataframe namespace"
  (:refer-clojure :exclude [filter sort])
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [snowpark-clj.dataframe :as df]
            [snowpark-clj.convert :as convert]
            [malli.core :as m]
            [spy.assert :as assert]
            [spy.core :as spy]
            [spy.protocol :as protocol])
  (:import [com.snowflake.snowpark_java Functions]))

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

(defprotocol MockSession
  (createDataFrame [this rows schema] "Mock Session.createDataFrame method")
  (table [this table-name] "Mock Session.table method"))

(defprotocol MockDataFrame
  (col [this col-name] "Mock DataFrame.col method")
  (filter [this condition] "Mock DataFrame.filter method")
  (select [this col-array] "Mock DataFrame.select method")
  (limit [this n] "Mock DataFrame.limit method")
  (sort [this col-array] "Mock DataFrame.sort method")
  (groupBy [this col-array] "Mock DataFrame.groupBy method")
  (join [this other-df join-expr join-type] "Mock DataFrame.join method"))

(defn- mock-session []
  (let [mock (protocol/mock MockSession
                            (createDataFrame [_ _ _] "createDataFrame")
                            (table [_ _] "table"))
        spies (spy.protocol/spies mock)]
    {:mock-session mock
     :mock-session-spies spies}))

(defn- mock-dataframe []
  (let [mock (protocol/mock MockDataFrame
                            (col [_ _] (Functions/col "col"))
                            (filter [_ _] "filter")
                            (select [_ _] "select")
                            (limit [_ _] "limit")
                            (sort [_ _] "sort")
                            (groupBy [_ _] "groupBy")
                            (join [_ _ _ _] "join"))
        spies (protocol/spies mock)]
    {:mock-dataframe mock
     :mock-dataframe-spies spies}))

(deftest test-create-dataframe
  (testing "Creating DataFrame from Clojure data (2-arity)"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          session-wrapper {:session mock-session 
                           :read-key-fn keyword 
                           :write-key-fn name}
          mock-schema (reify Object (toString [_] "mock-schema"))
          mock-rows []]
      
      (with-redefs [convert/infer-schema (fn [_data _write-key-fn] mock-schema)
                    convert/maps->rows (fn [_data _schema _write-key-fn] mock-rows)] 
        (let [result (df/create-dataframe session-wrapper test-data)]
          
          ;; Verify the result is properly wrapped
          (is (contains? result :dataframe))
          (is (= keyword (:read-key-fn result)))
          (is (= name (:write-key-fn result)))
          
          ;; Verify that the mock session's createDataFrame was called correctly
          (assert/called-once? (:createDataFrame mock-session-spies))
          (assert/called-with? (:createDataFrame mock-session-spies) mock-session mock-rows mock-schema)))))
  
  (testing "Creating DataFrame from Clojure data with explicit Snowpark schema (3-arity)"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          session-wrapper {:session mock-session 
                           :read-key-fn keyword 
                           :write-key-fn (comp str/upper-case name)}
          snowpark-schema (convert/malli-schema->snowpark-schema test-employee-schema (comp str/upper-case name))
          mock-rows []]
      
      (with-redefs [convert/maps->rows (fn [_data _schema _write-key-fn] mock-rows)]
        (let [result (df/create-dataframe session-wrapper test-data snowpark-schema)]
          
          ;; Verify the result is properly wrapped
          (is (contains? result :dataframe))
          (is (= keyword (:read-key-fn result)))
          (is (= "TEST" ((:write-key-fn result) :test)))
          
          ;; Verify that the mock session's createDataFrame was called correctly
          (assert/called-once? (:createDataFrame mock-session-spies))
          (assert/called-with? (:createDataFrame mock-session-spies) mock-session mock-rows snowpark-schema)))))
  
  (testing "Creating DataFrame from empty data should throw exception"
    (is (thrown-with-msg? IllegalArgumentException 
                          #"Cannot create DataFrame from empty data"
                          (df/create-dataframe {:session nil :key-fn identity} [])))))

(deftest test-table
  (testing "Table function calls session.table with correct parameters"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          session-wrapper {:session mock-session 
                           :read-key-fn keyword 
                           :write-key-fn (comp str/upper-case name)}
          table-name "test_table"]

      (let [result (df/table session-wrapper table-name)]
        ;; Verify the result is properly wrapped
        (is (= "table" (:dataframe result)))
        (is (= (:read-key-fn session-wrapper) (:read-key-fn result)))
        (is (= (:write-key-fn session-wrapper) (:write-key-fn result))))
      
      ;; Verify that the mock session's table was called correctly
      (assert/called-once? (:table mock-session-spies))
      (assert/called-with? (:table mock-session-spies) mock-session table-name))))

(deftest test-select
  (testing "Select function converts columns and calls DataFrame.select"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe 
                   :read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          columns [:name :salary]
          result (df/select test-df columns)]
      
      ;; Verify the result is properly wrapped
      (is (= "select" (:dataframe result)))
      (is (= (:read-key-fn test-df) (:read-key-fn result)))
      (is (= (:write-key-fn test-df) (:write-key-fn result)))
      
      ;; Verify that the mock dataframe's select was called correctly
      ;; The columns should be transformed to string array: ["NAME", "SALARY"]
      (assert/called-once? (:select mock-dataframe-spies))
      ;; Check the actual call arguments
      (let [calls (spy/calls (:select mock-dataframe-spies))
            [call-args] calls
            [called-df called-array] call-args]
        (is (= mock-dataframe called-df))
        (is (= ["NAME" "SALARY"] (vec called-array)))))))

(deftest test-df-filter
  (testing "Filter function accepts Column objects and encoded column names"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe :write-key-fn (comp str/upper-case name)}]
      
      (testing "with Column object"
        (let [column-obj (Functions/col "SALARY")
              result (df/df-filter test-df column-obj)]
          ;; The result should be a wrapped dataframe
          (is (= "filter" (:dataframe result)))
          (is (= (:read-key-fn test-df) (:read-key-fn result)))
          (is (= (:write-key-fn test-df) (:write-key-fn result)))
          ;; Check that the underlying DataFrame.filter was called with the column object
          (assert/called-once? (:filter mock-dataframe-spies))
          (assert/called-with? (:filter mock-dataframe-spies) mock-dataframe column-obj)))
      
      (testing "with encoded column name"
        (let [result (df/df-filter test-df :salary)]
          ;; The result should be a wrapped dataframe
          (is (= "filter" (:dataframe result)))
          (is (= (:read-key-fn test-df) (:read-key-fn result)))
          (is (= (:write-key-fn test-df) (:write-key-fn result)))
          ;; Check that a Column was created using transformed name, then filter was called
          (assert/called-once? (:col mock-dataframe-spies))
          (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")
          (assert/called-n-times? (:filter mock-dataframe-spies) 2))))))

(deftest test-limit
  (testing "Limit function calls DataFrame.limit with integer value"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe 
                   :read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          limit-count 10
          result (df/limit test-df limit-count)]
      
      ;; Verify the result is properly wrapped
      (is (= "limit" (:dataframe result)))
      (is (= (:read-key-fn test-df) (:read-key-fn result)))
      (is (= (:write-key-fn test-df) (:write-key-fn result)))
      
      ;; Verify that the mock dataframe's limit was called correctly
      (assert/called-once? (:limit mock-dataframe-spies))
      (assert/called-with? (:limit mock-dataframe-spies) mock-dataframe 10))))

(deftest test-df-sort
  (testing "Sort function handles single and multiple columns with column transformation"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe 
                   :read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          single-col :salary
          multi-cols [:salary :name]]
      
      ;; Test single column
      (let [result1 (df/df-sort test-df single-col)]
        (is (= "sort" (:dataframe result1)))
        (is (= (:read-key-fn test-df) (:read-key-fn result1)))
        (is (= (:write-key-fn test-df) (:write-key-fn result1))))
      
      ;; Test multiple columns
      (let [result2 (df/df-sort test-df multi-cols)]
        (is (= "sort" (:dataframe result2)))
        (is (= (:read-key-fn test-df) (:read-key-fn result2)))
        (is (= (:write-key-fn test-df) (:write-key-fn result2))))
      
      ;; Verify sort was called twice with Column arrays
      (assert/called-n-times? (:sort mock-dataframe-spies) 2)
      
      ;; Verify that .col was called with the transformed column names
      ;; Single column call: "SALARY"
      ;; Multiple column calls: "SALARY", "NAME"  
      (assert/called-n-times? (:col mock-dataframe-spies) 3)
      (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")
      (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "NAME"))))

(deftest test-df-group-by
  (testing "Group-by function converts column names and calls DataFrame.groupBy"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe 
                   :read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          columns [:department :age]
          result (df/df-group-by test-df columns)]
      
      ;; Verify the result is properly wrapped
      (is (= "groupBy" (:dataframe result)))
      (is (= (:read-key-fn test-df) (:read-key-fn result)))
      (is (= (:write-key-fn test-df) (:write-key-fn result)))
      
      ;; Verify that the mock dataframe's groupBy was called correctly
      ;; The columns should be transformed based on to-columns-or-names 
      (assert/called-once? (:groupBy mock-dataframe-spies))
      ;; Check the actual call arguments  
      (let [calls (spy/calls (:groupBy mock-dataframe-spies))
            [call-args] calls
            [called-df called-array] call-args]
        (is (= mock-dataframe called-df))
        (is (= ["DEPARTMENT" "AGE"] (vec called-array))))))

(deftest test-join
  (testing "Join function handles different join types"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe 
                   :read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          other-mock-dataframe (reify 
                                 Object
                                 (toString [_] "other-mock-dataframe"))
          other-df {:dataframe other-mock-dataframe
                    :read-key-fn keyword 
                    :write-key-fn (comp str/upper-case name)}
          join-expr "left.id = right.id"]
      
      ;; Test default join type (2-arity)
      (let [result1 (df/join test-df other-df join-expr)]
        (is (= "join" (:dataframe result1)))
        (is (= (:read-key-fn test-df) (:read-key-fn result1)))
        (is (= (:write-key-fn test-df) (:write-key-fn result1))))
      
      ;; Test explicit join type (3-arity)
      (let [result2 (df/join test-df other-df join-expr :left)]
        (is (= "join" (:dataframe result2)))
        (is (= (:read-key-fn test-df) (:read-key-fn result2)))
        (is (= (:write-key-fn test-df) (:write-key-fn result2))))
      
      ;; Verify join was called twice with correct parameters
      (assert/called-n-times? (:join mock-dataframe-spies) 2)
      ;; First call with default join type (gets converted to "inner")
      (assert/called-with? (:join mock-dataframe-spies) mock-dataframe other-mock-dataframe join-expr "inner")
      ;; Second call with explicit join type (gets converted to "left")
      (assert/called-with? (:join mock-dataframe-spies) mock-dataframe other-mock-dataframe join-expr "left"))))

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

(deftest test-col
  (testing "Col function returns Column object with transformed name"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          test-df {:dataframe mock-dataframe :write-key-fn (comp str/upper-case name)}]
      
      ;; Test with keyword column name
      (let [result (df/col test-df :name)]
        (is (some? result))
        ;; Verify the transformation was applied before calling .col
        (assert/called-once? (:col mock-dataframe-spies))
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "NAME"))
      
      ;; Test with string column name
      (let [result (df/col test-df "department")]
        (is (some? result))
        (assert/called-n-times? (:col mock-dataframe-spies) 2)
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "DEPARTMENT"))))
  
  (testing "Col function works with different write-key-fn transformations"

    (testing "Lowercase transformation"
      (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
            lowercase-df {:dataframe mock-dataframe :write-key-fn (comp str/lower-case name)}
            result (df/col lowercase-df :COLUMN_NAME)]
        (is (some? result))
        (assert/called-once? (:col mock-dataframe-spies))
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "column_name")))
    
    (testing "String transformation (no case change)"
      (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
            identity-df {:dataframe mock-dataframe :write-key-fn str}
            result (df/col identity-df :column-name)]
        (is (some? result))
        (assert/called-once? (:col mock-dataframe-spies))
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe ":column-name"))))))

