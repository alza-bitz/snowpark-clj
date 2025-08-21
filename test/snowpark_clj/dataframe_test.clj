(ns snowpark-clj.dataframe-test
  "Unit tests for the dataframe namespace" 
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [malli.generator :as mg]
   [snowpark-clj.convert :as convert]
   [snowpark-clj.dataframe :as df]
   [snowpark-clj.mocks :as mocks]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.schemas :as schemas]
   [snowpark-clj.wrapper :as wrapper]
   [spy.assert :as assert]
   [spy.core :as spy]
   [spy.protocol :as protocol])
  (:import
   [com.snowflake.snowpark_java Functions]))

(defprotocol MockSession
  (createDataFrame [this rows schema] "Mock Session.createDataFrame method")
  (table [this table-name] "Mock Session.table method"))

(defprotocol MockDataFrameWriter
  (saveAsTable [this table-name] "Mock DataFrameWriter.saveAsTable method")
  (mode [this mode-str] "Mock DataFrameWriter.mode method")
  (options [this options-map] "Mock DataFrameWriter.options method"))

(defn- mock-session []
  (let [mock (protocol/mock MockSession
                            (createDataFrame [_ _ _] "createDataFrame")
                            (table [_ _] "table"))]
    {:mock-session mock
     :mock-session-spies (protocol/spies mock)}))

(defn- mock-session-wrapper [mock-session opts]
  (let [mock (protocol/mock wrapper/IWrappedSessionOptions
                            (unwrap [_] mock-session)
                            (unwrap-option [_ option-key] (get opts option-key))
                            (unwrap-options [_] opts))]
    {:mock-session-wrapper mock
     :mock-session-wrapper-spies (protocol/spies mock)}))

(defn- mock-dataframe-writer []
  (let [mock-with-options (protocol/mock MockDataFrameWriter
                                      (saveAsTable [_ _] "saveAsTable")
                                      (mode [_ _] nil)
                                      (options [_ _] nil))
        mock-with-mode (protocol/mock MockDataFrameWriter
                            (saveAsTable [_ _] "saveAsTable")
                            (mode [_ _] nil)
                            (options [_ _] mock-with-options))
        mock (protocol/mock MockDataFrameWriter
                            (saveAsTable [_ _] "saveAsTable")
                            (mode [_ _] mock-with-mode)
                            (options [_ _] nil))]
    {:mock-writer mock
     :mock-writer-spies (protocol/spies mock)
     :mock-writer-with-mode mock-with-mode
     :mock-writer-with-mode-spies (protocol/spies mock-with-mode)
     :mock-writer-with-options mock-with-options
     :mock-writer-with-options-spies (protocol/spies mock-with-options)}))

(defn- mock-dataframe-wrapper
  "Returns a minimal wrapper for tests that don't need map access."
  [df opts]
  (reify
    wrapper/IWrappedSessionOptions
    (unwrap [_] df)
    (unwrap-option [_ option-key] (get opts option-key))
    (unwrap-options [_] opts)))

(deftest test-create-dataframe
  (testing "Create dataframe from a collection of maps"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          opts {:col->key-fn keyword :key->col-fn name}
          {:keys [mock-session-wrapper]} (mock-session-wrapper mock-session opts)
          data (mg/generate [:vector {:gen/min 1 :gen/max 5} schemas/employee-schema])
          mock-schema (mocks/mock-schema [])
          mock-rows []]
  
      (with-redefs [schema/infer-schema (spy/stub mock-schema)
                    convert/maps->rows (spy/stub mock-rows)]
        (let [result (df/create-dataframe mock-session-wrapper data)]

          ;; Verify the result is properly wrapped
          (is (= "createDataFrame" (wrapper/unwrap result)))
          (is (= keyword (wrapper/unwrap-option result :col->key-fn)))
          (is (= name (wrapper/unwrap-option result :key->col-fn)))

          (assert/called-once-with? schema/infer-schema mock-session-wrapper data)
          (assert/called-once-with? convert/maps->rows data mock-schema (:key->col-fn opts))
          ;; Verify that the mock session's createDataFrame was called correctly
          (assert/called-once-with? (:createDataFrame mock-session-spies) mock-session mock-rows mock-schema)))))
  
  (testing "Create dataframe from a collection of maps and schema"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          opts {:col->key-fn keyword :key->col-fn name}
          {:keys [mock-session-wrapper]} (mock-session-wrapper mock-session opts)
          data (mg/generate [:vector {:gen/min 1 :gen/max 5} schemas/employee-schema-with-optional-keys])
          {:keys [mock-schema]} (mocks/mock-schema [])
          mock-rows []]
  
      (with-redefs [convert/maps->rows (spy/stub mock-rows)]
        (let [result (df/create-dataframe mock-session-wrapper data mock-schema)]
  
          ;; Verify the result is properly wrapped
          (is (= "createDataFrame" (wrapper/unwrap result)))
          (is (= keyword (wrapper/unwrap-option result :col->key-fn)))
          (is (= name (wrapper/unwrap-option result :key->col-fn)))
          
          (assert/called-once-with? convert/maps->rows data mock-schema (:key->col-fn opts))
          ;; Verify that the mock session's createDataFrame was called correctly
          (assert/called-once-with? (:createDataFrame mock-session-spies) mock-session mock-rows mock-schema)))))
  
  (testing "Create dataframe from empty data should throw exception"
    (is (thrown-with-msg? IllegalArgumentException 
                          #"Cannot create dataframe from empty data"
                          (df/create-dataframe (mock-session-wrapper (mock-session) {}) [])))))

(deftest test-table
  (testing "Table function calls session.table with correct parameters"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          opts {:col->key-fn keyword
                :key->col-fn (comp str/upper-case name)}
          {:keys [mock-session-wrapper]} (mock-session-wrapper mock-session opts)
          table-name "test_table"]

      (let [result (df/table mock-session-wrapper table-name)]
        ;; Verify the result is properly wrapped
        (is (= "table" (wrapper/unwrap result)))
        (is (= (:col->key-fn opts) (wrapper/unwrap-option result :col->key-fn)))
        (is (= (:key->col-fn opts) (wrapper/unwrap-option result :key->col-fn))))

      ;; Verify that the mock session's table was called correctly
      (assert/called-once? (:table mock-session-spies))
      (assert/called-with? (:table mock-session-spies) mock-session table-name))))

(deftest test-select
  (testing "Select function converts columns and calls DataFrame.select"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
          columns [:name :salary]
          result (df/select (mock-dataframe-wrapper mock-dataframe opts) columns)]
      
      ;; Verify the result is properly wrapped
      (is (= "select" (wrapper/unwrap result)))
      (is (= (:col->key-fn opts) (wrapper/unwrap-option result :col->key-fn)))
      (is (= (:key->col-fn opts) (wrapper/unwrap-option result :key->col-fn)))
      
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
  (testing "Filter function accepts Column objects and column keys"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:key->col-fn (comp str/upper-case name)}]
      
      (testing "with Column object"
        (let [column-obj (Functions/col "SALARY")
              result (df/df-filter (mock-dataframe-wrapper mock-dataframe opts) column-obj)]
          ;; The result should be a wrapped dataframe
          (is (= "filter" (wrapper/unwrap result)))
          (is (= (:col->key-fn opts) (wrapper/unwrap-option result :col->key-fn)))
          (is (= (:key->col-fn opts) (wrapper/unwrap-option result :key->col-fn)))
          ;; Check that the underlying DataFrame.filter was called with the column object
          (assert/called-once? (:filter mock-dataframe-spies))
          (assert/called-with? (:filter mock-dataframe-spies) mock-dataframe column-obj)))
      
      (testing "with column key"
        (let [result (df/df-filter (mock-dataframe-wrapper mock-dataframe opts) :salary)]
          ;; The result should be a wrapped dataframe
          (is (= "filter" (wrapper/unwrap result)))
          (is (= (:col->key-fn opts) (wrapper/unwrap-option result :col->key-fn)))
          (is (= (:key->col-fn opts) (wrapper/unwrap-option result :key->col-fn)))
          ;; Check that a Column was created using column name, then filter was called
          (assert/called-once? (:col mock-dataframe-spies))
          (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")
          (assert/called-n-times? (:filter mock-dataframe-spies) 2))))))

(deftest test-limit
  (testing "Limit function calls DataFrame.limit with integer value"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
          limit-count 10
          result (df/limit (mock-dataframe-wrapper mock-dataframe opts) limit-count)]
      
      ;; Verify the result is properly wrapped
      (is (= "limit" (wrapper/unwrap result)))
      (is (= (:col->key-fn opts) (wrapper/unwrap-option result :col->key-fn)))
      (is (= (:key->col-fn opts) (wrapper/unwrap-option result :key->col-fn)))
      
      ;; Verify that the mock dataframe's limit was called correctly
      (assert/called-once? (:limit mock-dataframe-spies))
      (assert/called-with? (:limit mock-dataframe-spies) mock-dataframe 10))))

(deftest test-df-sort
  (testing "Sort function handles single and multiple columns with column keys"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
          single-col :salary
          multi-cols [:salary :name]]
      
      ;; Test single column
      (let [result1 (df/df-sort (mock-dataframe-wrapper mock-dataframe opts) single-col)]
        (is (= "sort" (wrapper/unwrap result1)))
        (is (= (:col->key-fn opts) (wrapper/unwrap-option result1 :col->key-fn)))
        (is (= (:key->col-fn opts) (wrapper/unwrap-option result1 :key->col-fn))))
      
      ;; Test multiple columns
      (let [result2 (df/df-sort (mock-dataframe-wrapper mock-dataframe opts) multi-cols)]
        (is (= "sort" (wrapper/unwrap result2)))
        (is (= (:col->key-fn opts) (wrapper/unwrap-option result2 :col->key-fn)))
        (is (= (:key->col-fn opts) (wrapper/unwrap-option result2 :key->col-fn))))
      
      ;; Verify sort was called twice with Column arrays
      (assert/called-n-times? (:sort mock-dataframe-spies) 2)
      
      ;; Verify that .col was called with the column names
      ;; Single column call: "SALARY"
      ;; Multiple column calls: "SALARY", "NAME"
      (assert/called-n-times? (:col mock-dataframe-spies) 3)
      (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")
      (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "NAME"))))

(deftest test-df-group-by
  (testing "Group-by function converts column names and calls DataFrame.groupBy"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe) 
          opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
          columns [:department :age]
          result (df/df-group-by (mock-dataframe-wrapper mock-dataframe opts) columns)]
      
      ;; Verify the result is properly wrapped
      (is (= "groupBy" (wrapper/unwrap result)))
      (is (= (:col->key-fn opts) (wrapper/unwrap-option result :col->key-fn)))
      (is (= (:key->col-fn opts) (wrapper/unwrap-option result :key->col-fn)))
      
      ;; Verify that the mock dataframe's groupBy was called correctly
      ;; The columns should be transformed based on to-columns-or-names 
      (assert/called-once? (:groupBy mock-dataframe-spies))
      ;; Check the actual call arguments  
      (let [calls (spy/calls (:groupBy mock-dataframe-spies))
            [call-args] calls
            [called-df called-array] call-args]
        (is (= mock-dataframe called-df))
        (is (= ["DEPARTMENT" "AGE"] (vec called-array)))))))

(deftest test-join
  (testing "Join function handles different join types"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
          other-mock-dataframe (reify 
                                 Object
                                 (toString [_] "other-mock-dataframe"))
          join-expr "left.id = right.id"]
      
      ;; Test default join type (2-arity)
      (let [result1 (df/join (mock-dataframe-wrapper mock-dataframe opts) (mock-dataframe-wrapper other-mock-dataframe opts) join-expr)]
        (is (= "join" (wrapper/unwrap result1)))
        (is (= (:col->key-fn opts) (wrapper/unwrap-option result1 :col->key-fn)))
        (is (= (:key->col-fn opts) (wrapper/unwrap-option result1 :key->col-fn))))
      
      ;; Test explicit join type (3-arity)
      (let [result2 (df/join (mock-dataframe-wrapper mock-dataframe opts) (mock-dataframe-wrapper other-mock-dataframe opts) join-expr :left)]
        (is (= "join" (wrapper/unwrap result2)))
        (is (= (:col->key-fn opts) (wrapper/unwrap-option result2 :col->key-fn)))
        (is (= (:key->col-fn opts) (wrapper/unwrap-option result2 :key->col-fn))))
      
      ;; Verify join was called twice with correct parameters
      (assert/called-n-times? (:join mock-dataframe-spies) 2)
      ;; First call with default join type (gets converted to "inner")
      (assert/called-with? (:join mock-dataframe-spies) mock-dataframe other-mock-dataframe join-expr "inner")
      ;; Second call with explicit join type (gets converted to "left")
      (assert/called-with? (:join mock-dataframe-spies) mock-dataframe other-mock-dataframe join-expr "left"))))

(deftest test-collect
  (testing "Collect function converts DataFrame to maps with proper key-fn"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword}
          expected-result [{:id 1 :name "Alice"}]]
      
      (with-redefs [convert/rows->maps (spy/stub expected-result)]
        (let [result (df/collect (mock-dataframe-wrapper mock-dataframe opts))]
          ;; Verify the result is the data returned by mock collect
          (is (= expected-result result))

          ;; Verify that the mock dataframe's collect was called correctly
          (assert/called-once? (:collect mock-dataframe-spies))
          (assert/called-with? (:collect mock-dataframe-spies) mock-dataframe)

          (assert/called-once? convert/rows->maps)
          (assert/called-with? convert/rows->maps "mock-rows" "mock-schema" keyword))))))

(deftest test-show
  (testing "Show function calls DataFrame.show with correct parameters"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword 
                   :key->col-fn (comp str/upper-case name)}]
      
      ;; Test with default row count (1-arity)
      (df/show (mock-dataframe-wrapper mock-dataframe opts))
      
      ;; Test with explicit row count (2-arity)
      (df/show (mock-dataframe-wrapper mock-dataframe opts) 5)
      
      ;; Verify that the mock dataframe's show was called correctly
      (assert/called-n-times? (:show mock-dataframe-spies) 2)
      (assert/called-with? (:show mock-dataframe-spies) mock-dataframe 20)  ; default
      (assert/called-with? (:show mock-dataframe-spies) mock-dataframe 5))))

(deftest test-row-count
  (testing "Count function calls DataFrame.count"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword 
                   :key->col-fn (comp str/upper-case name)}
          result (df/row-count (mock-dataframe-wrapper mock-dataframe opts))]
      
      ;; Verify the result is the count returned by mock
      (is (= "mock-count" result))
      
      ;; Verify that the mock dataframe's count was called correctly
      (assert/called-once? (:count mock-dataframe-spies))
      (assert/called-with? (:count mock-dataframe-spies) mock-dataframe))))

(deftest test-df-take
  (testing "Take function handles correct arity"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe {:lazy-chain? true})
          opts {:col->key-fn keyword}
          expected-result [{:id 1 :name "Alice"}]]
      
      (with-redefs [convert/rows->maps (spy/stub expected-result)]
        (let [result (df/df-take (mock-dataframe-wrapper mock-dataframe opts) 1)]

          ;; df-take uses limit + collect internally, so we verify the result
          (is (= expected-result result))

          ;; Verify that limit was called with the correct parameter on the limit mock
          (assert/called-once? (:limit mock-dataframe-spies))
          (assert/called-with? (:limit mock-dataframe-spies) mock-dataframe 1)

          (assert/called-once? convert/rows->maps)
          (assert/called-with? convert/rows->maps "mock-rows" "mock-schema" keyword))))))

(deftest test-save-as-table
  (testing "Save-as-table function handles different modes and options"
    (let [table-name "test_table"]
      
      ;; Test simple save (2-arity)
      (testing "2-arity version"
        (let [{:keys [mock-writer mock-writer-spies]} (mock-dataframe-writer)
              {:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe {:mock-writer mock-writer})
              opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
              result (df/save-as-table (mock-dataframe-wrapper mock-dataframe opts) table-name)]
          (is (= "saveAsTable" result))
          (assert/called-once? (:write mock-dataframe-spies))
          (assert/called-once? (:saveAsTable mock-writer-spies))))
      
      ;; Test with mode (3-arity)
      (testing "3-arity version"
        (let [{:keys [mock-writer mock-writer-spies mock-writer-with-mode-spies]} (mock-dataframe-writer)
              {:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe {:mock-writer mock-writer})
              opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
              result (df/save-as-table (mock-dataframe-wrapper mock-dataframe opts) table-name :overwrite)]
          (is (= "saveAsTable" result))
          (assert/called-once? (:write mock-dataframe-spies))
          (assert/called-once? (:mode mock-writer-spies))
          (assert/called-once? (:saveAsTable mock-writer-with-mode-spies))))
      
      ;; Test with mode and options (4-arity)
      (testing "4-arity version"
        (let [{:keys [mock-writer mock-writer-spies 
                      mock-writer-with-mode-spies mock-writer-with-options-spies]} (mock-dataframe-writer)
              {:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe {:mock-writer mock-writer})
              opts {:col->key-fn keyword :key->col-fn (comp str/upper-case name)}
              result (df/save-as-table (mock-dataframe-wrapper mock-dataframe opts) table-name :overwrite {:cluster-by ["col1"]})]
          (is (= "saveAsTable" result))
          (assert/called-once? (:write mock-dataframe-spies))
          (assert/called-once? (:mode mock-writer-spies))
          (assert/called-once? (:options mock-writer-with-mode-spies))
          (assert/called-once? (:saveAsTable mock-writer-with-options-spies)))))))

(deftest test-schema
  (testing "Schema function returns DataFrame schema"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:col->key-fn keyword 
                   :key->col-fn (comp str/upper-case name)}
          result (df/schema (mock-dataframe-wrapper mock-dataframe opts))]
      
      ;; Verify the result is the schema returned by mock
      (is (= "mock-schema" result))
      
      ;; Verify that the mock dataframe's schema was called correctly
      (assert/called-once? (:schema mock-dataframe-spies))
      (assert/called-with? (:schema mock-dataframe-spies) mock-dataframe))))

(deftest test-col
  (testing "Col function returns Column object with transformed name"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
          opts {:key->col-fn (comp str/upper-case name)}]
      
      ;; Test with keyword column name
      (let [result (df/col (mock-dataframe-wrapper mock-dataframe opts) :name)]
        (is (some? result))
        ;; Verify the column name decoding was applied before calling .col
        (assert/called-once? (:col mock-dataframe-spies))
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "NAME"))
      
      ;; Test with string column name
      (let [result (df/col (mock-dataframe-wrapper mock-dataframe opts) "department")]
        (is (some? result))
        (assert/called-n-times? (:col mock-dataframe-spies) 2)
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "DEPARTMENT"))))
  
  (testing "Col function works with different key->col-fn decodings"

    (testing "Lowercase decoding"
      (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
            lowercase-df {:dataframe mock-dataframe :key->col-fn (comp str/lower-case name)}
            result (df/col (mock-dataframe-wrapper mock-dataframe lowercase-df) :COLUMN_NAME)]
        (is (some? result))
        (assert/called-once? (:col mock-dataframe-spies))
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "column_name")))
    
    (testing "String decoding (no case change)"
      (let [{:keys [mock-dataframe mock-dataframe-spies]} (mocks/mock-dataframe)
            opts {:key->col-fn str}
            result (df/col (mock-dataframe-wrapper mock-dataframe opts) :column-name)]
        (is (some? result))
        (assert/called-once? (:col mock-dataframe-spies))
                (assert/called-with? (:col mock-dataframe-spies) mock-dataframe ":column-name")))))
