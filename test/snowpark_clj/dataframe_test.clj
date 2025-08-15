(ns snowpark-clj.dataframe-test
  "Unit tests for the dataframe namespace"
  (:refer-clojure :exclude [filter sort count])
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [malli.core :as m]
   [snowpark-clj.convert :as convert]
   [snowpark-clj.dataframe :as df]
   [snowpark-clj.mocks :as mocks]
   [snowpark-clj.schema :as schema]
   [snowpark-clj.wrapper :as wrapper]
   [spy.assert :as assert]
   [spy.core :as spy]
   [spy.protocol :as protocol])
  (:import
   [com.snowflake.snowpark_java Functions]))

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
  (join [this other-df join-expr join-type] "Mock DataFrame.join method")
  (collect [this] "Mock DataFrame.collect method")
  (show [this n] "Mock DataFrame.show method")
  (count [this] "Mock DataFrame.count method")
  (schema [this] "Mock DataFrame.schema method")
  (write [this] "Mock DataFrame.write method"))

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

(defn- mock-dataframe 
  [& {:keys [lazy-chain? mock-writer mock-schema]
      :or {lazy-chain? false mock-writer "mock-writer" mock-schema "mock-schema"}}]
  (let [mock (protocol/mock MockDataFrame
                            (col [_ _] (Functions/col "col"))
                            (filter [this _] (if lazy-chain? this "filter"))
                            (select [this _] (if lazy-chain? this "select"))
                            (limit [this _] (if lazy-chain? this "limit"))
                            (sort [this _] (if lazy-chain? this "sort"))
                            (groupBy [this _] (if lazy-chain? this "groupBy"))
                            (join [this _ _ _] (if lazy-chain? this "join"))
                            (collect [_] "mock-rows")
                            (show [_ _] nil)
                            (count [_] "mock-count")
                            (schema [_] mock-schema)
                            (write [_] mock-writer))]
    {:mock-dataframe mock
     :mock-dataframe-spies (protocol/spies mock)}))

(defn- mock-dataframe-wrapper
  "Returns a minimal wrapper for tests that don't need map access."
  [df opts]
  (reify
    wrapper/IWrappedSessionOptions
    (unwrap [_] df)
    (unwrap-option [_ option-key] (get opts option-key))
    (unwrap-options [_] opts)))

(deftest test-create-dataframe
  (testing "Creating DataFrame from Clojure data (2-arity)"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          opts {:read-key-fn keyword
                :write-key-fn name}
          {:keys [mock-session-wrapper]} (mock-session-wrapper mock-session opts)
          mock-schema (reify Object (toString [_] "mock-schema"))
          mock-rows []]
  
      (with-redefs [schema/infer-schema (fn [_session _data] mock-schema)
                    convert/maps->rows (fn [_data _schema _write-key-fn] mock-rows)]
        (let [result (df/create-dataframe mock-session-wrapper test-data)]
  
          ;; Verify the result is properly wrapped
          (is (= "createDataFrame" (wrapper/unwrap result)))
          (is (= keyword (wrapper/unwrap-option result :read-key-fn)))
          (is (= name (wrapper/unwrap-option result :write-key-fn)))
  
          ;; Verify that the mock session's createDataFrame was called correctly
          (assert/called-once? (:createDataFrame mock-session-spies))
          (assert/called-with? (:createDataFrame mock-session-spies) mock-session mock-rows mock-schema)))))
  
  (testing "Creating DataFrame from Clojure data with explicit Snowpark schema (3-arity)"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          opts {:read-key-fn keyword
                :write-key-fn name}
          {:keys [mock-session-wrapper]} (mock-session-wrapper mock-session opts)
          snowpark-schema (schema/malli-schema->snowpark-schema mock-session-wrapper test-employee-schema)
          mock-rows []]
  
      (with-redefs [convert/maps->rows (fn [_data _schema _write-key-fn] mock-rows)]
        (let [result (df/create-dataframe mock-session-wrapper test-data snowpark-schema)]
  
          ;; Verify the result is properly wrapped
          (is (= "createDataFrame" (wrapper/unwrap result)))
          (is (= keyword (wrapper/unwrap-option result :read-key-fn)))
          (is (= name (wrapper/unwrap-option result :write-key-fn)))
  
          ;; Verify that the mock session's createDataFrame was called correctly
          (assert/called-once? (:createDataFrame mock-session-spies))
          (assert/called-with? (:createDataFrame mock-session-spies) mock-session mock-rows snowpark-schema)))))
  
  (testing "Creating DataFrame from empty data should throw exception"
    (is (thrown-with-msg? IllegalArgumentException 
                          #"Cannot create dataframe from empty data"
                          (df/create-dataframe (mock-session-wrapper (mock-session) {}) [])))))

(deftest test-table
  (testing "Table function calls session.table with correct parameters"
    (let [{:keys [mock-session mock-session-spies]} (mock-session)
          opts {:read-key-fn keyword
                :write-key-fn (comp str/upper-case name)}
          {:keys [mock-session-wrapper]} (mock-session-wrapper mock-session opts)
          table-name "test_table"]

      (let [result (df/table mock-session-wrapper table-name)]
        ;; Verify the result is properly wrapped
        (is (= "table" (wrapper/unwrap result)))
        (is (= (:read-key-fn opts) (wrapper/unwrap-option result :read-key-fn)))
        (is (= (:write-key-fn opts) (wrapper/unwrap-option result :write-key-fn))))

      ;; Verify that the mock session's table was called correctly
      (assert/called-once? (:table mock-session-spies))
      (assert/called-with? (:table mock-session-spies) mock-session table-name))))

(deftest test-select
  (testing "Select function converts columns and calls DataFrame.select"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
          columns [:name :salary]
          result (df/select (mock-dataframe-wrapper mock-dataframe opts) columns)]
      
      ;; Verify the result is properly wrapped
      (is (= "select" (wrapper/unwrap result)))
      (is (= (:read-key-fn opts) (wrapper/unwrap-option result :read-key-fn)))
      (is (= (:write-key-fn opts) (wrapper/unwrap-option result :write-key-fn)))
      
      ;; Verify that the mock dataframe's select was called correctly
      ;; The columns should be decoded to string array: ["NAME", "SALARY"]
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
          opts {:write-key-fn (comp str/upper-case name)}]
      
      (testing "with Column object"
        (let [column-obj (Functions/col "SALARY")
              result (df/df-filter (mock-dataframe-wrapper mock-dataframe opts) column-obj)]
          ;; The result should be a wrapped dataframe
          (is (= "filter" (wrapper/unwrap result)))
          (is (= (:read-key-fn opts) (wrapper/unwrap-option result :read-key-fn)))
          (is (= (:write-key-fn opts) (wrapper/unwrap-option result :write-key-fn)))
          ;; Check that the underlying DataFrame.filter was called with the column object
          (assert/called-once? (:filter mock-dataframe-spies))
          (assert/called-with? (:filter mock-dataframe-spies) mock-dataframe column-obj)))
      
      (testing "with encoded column name"
        (let [result (df/df-filter (mock-dataframe-wrapper mock-dataframe opts) :salary)]
          ;; The result should be a wrapped dataframe
          (is (= "filter" (wrapper/unwrap result)))
          (is (= (:read-key-fn opts) (wrapper/unwrap-option result :read-key-fn)))
          (is (= (:write-key-fn opts) (wrapper/unwrap-option result :write-key-fn)))
          ;; Check that a Column was created using decoded name, then filter was called
          (assert/called-once? (:col mock-dataframe-spies))
          (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")
          (assert/called-n-times? (:filter mock-dataframe-spies) 2))))))

(deftest test-limit
  (testing "Limit function calls DataFrame.limit with integer value"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
          limit-count 10
          result (df/limit (mock-dataframe-wrapper mock-dataframe opts) limit-count)]
      
      ;; Verify the result is properly wrapped
      (is (= "limit" (wrapper/unwrap result)))
      (is (= (:read-key-fn opts) (wrapper/unwrap-option result :read-key-fn)))
      (is (= (:write-key-fn opts) (wrapper/unwrap-option result :write-key-fn)))
      
      ;; Verify that the mock dataframe's limit was called correctly
      (assert/called-once? (:limit mock-dataframe-spies))
      (assert/called-with? (:limit mock-dataframe-spies) mock-dataframe 10))))

(deftest test-df-sort
  (testing "Sort function handles single and multiple columns with column names decoded"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
          single-col :salary
          multi-cols [:salary :name]]
      
      ;; Test single column
      (let [result1 (df/df-sort (mock-dataframe-wrapper mock-dataframe opts) single-col)]
        (is (= "sort" (wrapper/unwrap result1)))
        (is (= (:read-key-fn opts) (wrapper/unwrap-option result1 :read-key-fn)))
        (is (= (:write-key-fn opts) (wrapper/unwrap-option result1 :write-key-fn))))
      
      ;; Test multiple columns
      (let [result2 (df/df-sort (mock-dataframe-wrapper mock-dataframe opts) multi-cols)]
        (is (= "sort" (wrapper/unwrap result2)))
        (is (= (:read-key-fn opts) (wrapper/unwrap-option result2 :read-key-fn)))
        (is (= (:write-key-fn opts) (wrapper/unwrap-option result2 :write-key-fn))))
      
      ;; Verify sort was called twice with Column arrays
      (assert/called-n-times? (:sort mock-dataframe-spies) 2)
      
      ;; Verify that .col was called with the decoded column names
      ;; Single column call: "SALARY"
      ;; Multiple column calls: "SALARY", "NAME"
      (assert/called-n-times? (:col mock-dataframe-spies) 3)
      (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "SALARY")
      (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "NAME"))))

(deftest test-df-group-by
  (testing "Group-by function converts column names and calls DataFrame.groupBy"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe) 
          opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
          columns [:department :age]
          result (df/df-group-by (mock-dataframe-wrapper mock-dataframe opts) columns)]
      
      ;; Verify the result is properly wrapped
      (is (= "groupBy" (wrapper/unwrap result)))
      (is (= (:read-key-fn opts) (wrapper/unwrap-option result :read-key-fn)))
      (is (= (:write-key-fn opts) (wrapper/unwrap-option result :write-key-fn)))
      
      ;; Verify that the mock dataframe's groupBy was called correctly
      ;; The columns should be decoded based on to-columns-or-names 
      (assert/called-once? (:groupBy mock-dataframe-spies))
      ;; Check the actual call arguments  
      (let [calls (spy/calls (:groupBy mock-dataframe-spies))
            [call-args] calls
            [called-df called-array] call-args]
        (is (= mock-dataframe called-df))
        (is (= ["DEPARTMENT" "AGE"] (vec called-array)))))))

(deftest test-join
  (testing "Join function handles different join types"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
          other-mock-dataframe (reify 
                                 Object
                                 (toString [_] "other-mock-dataframe"))
          join-expr "left.id = right.id"]
      
      ;; Test default join type (2-arity)
      (let [result1 (df/join (mock-dataframe-wrapper mock-dataframe opts) (mock-dataframe-wrapper other-mock-dataframe opts) join-expr)]
        (is (= "join" (wrapper/unwrap result1)))
        (is (= (:read-key-fn opts) (wrapper/unwrap-option result1 :read-key-fn)))
        (is (= (:write-key-fn opts) (wrapper/unwrap-option result1 :write-key-fn))))
      
      ;; Test explicit join type (3-arity)
      (let [result2 (df/join (mock-dataframe-wrapper mock-dataframe opts) (mock-dataframe-wrapper other-mock-dataframe opts) join-expr :left)]
        (is (= "join" (wrapper/unwrap result2)))
        (is (= (:read-key-fn opts) (wrapper/unwrap-option result2 :read-key-fn)))
        (is (= (:write-key-fn opts) (wrapper/unwrap-option result2 :write-key-fn))))
      
      ;; Verify join was called twice with correct parameters
      (assert/called-n-times? (:join mock-dataframe-spies) 2)
      ;; First call with default join type (gets converted to "inner")
      (assert/called-with? (:join mock-dataframe-spies) mock-dataframe other-mock-dataframe join-expr "inner")
      ;; Second call with explicit join type (gets converted to "left")
      (assert/called-with? (:join mock-dataframe-spies) mock-dataframe other-mock-dataframe join-expr "left"))))

(deftest test-collect
  (testing "Collect function converts DataFrame to maps with proper key-fn"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword}
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
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}]
      
      ;; Test with default row count (1-arity)
      (df/show (mock-dataframe-wrapper mock-dataframe opts))
      
      ;; Test with explicit row count (2-arity)
      (df/show (mock-dataframe-wrapper mock-dataframe opts) 5)
      
      ;; Verify that the mock dataframe's show was called correctly
      (assert/called-n-times? (:show mock-dataframe-spies) 2)
      (assert/called-with? (:show mock-dataframe-spies) mock-dataframe 20)  ; default
      (assert/called-with? (:show mock-dataframe-spies) mock-dataframe 5))))

(deftest test-df-count
  (testing "Count function calls DataFrame.count"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          result (df/df-count (mock-dataframe-wrapper mock-dataframe opts))]
      
      ;; Verify the result is the count returned by mock
      (is (= "mock-count" result))
      
      ;; Verify that the mock dataframe's count was called correctly
      (assert/called-once? (:count mock-dataframe-spies))
      (assert/called-with? (:count mock-dataframe-spies) mock-dataframe))))

(deftest test-df-take
  (testing "Take function handles correct arity"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe {:lazy-chain? true})
          opts {:read-key-fn keyword}
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
              {:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe {:mock-writer mock-writer})
              opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
              result (df/save-as-table (mock-dataframe-wrapper mock-dataframe opts) table-name)]
          (is (= "saveAsTable" result))
          (assert/called-once? (:write mock-dataframe-spies))
          (assert/called-once? (:saveAsTable mock-writer-spies))))
      
      ;; Test with mode (3-arity)
      (testing "3-arity version"
        (let [{:keys [mock-writer mock-writer-spies mock-writer-with-mode-spies]} (mock-dataframe-writer)
              {:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe {:mock-writer mock-writer})
              opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
              result (df/save-as-table (mock-dataframe-wrapper mock-dataframe opts) table-name :overwrite)]
          (is (= "saveAsTable" result))
          (assert/called-once? (:write mock-dataframe-spies))
          (assert/called-once? (:mode mock-writer-spies))
          (assert/called-once? (:saveAsTable mock-writer-with-mode-spies))))
      
      ;; Test with mode and options (4-arity)
      (testing "4-arity version"
        (let [{:keys [mock-writer mock-writer-spies 
                      mock-writer-with-mode-spies mock-writer-with-options-spies]} (mock-dataframe-writer)
              {:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe {:mock-writer mock-writer})
              opts {:read-key-fn keyword :write-key-fn (comp str/upper-case name)}
              result (df/save-as-table (mock-dataframe-wrapper mock-dataframe opts) table-name :overwrite {:cluster-by ["col1"]})]
          (is (= "saveAsTable" result))
          (assert/called-once? (:write mock-dataframe-spies))
          (assert/called-once? (:mode mock-writer-spies))
          (assert/called-once? (:options mock-writer-with-mode-spies))
          (assert/called-once? (:saveAsTable mock-writer-with-options-spies)))))))

(deftest test-schema
  (testing "Schema function returns DataFrame schema"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:read-key-fn keyword 
                   :write-key-fn (comp str/upper-case name)}
          result (df/schema (mock-dataframe-wrapper mock-dataframe opts))]
      
      ;; Verify the result is the schema returned by mock
      (is (= "mock-schema" result))
      
      ;; Verify that the mock dataframe's schema was called correctly
      (assert/called-once? (:schema mock-dataframe-spies))
      (assert/called-with? (:schema mock-dataframe-spies) mock-dataframe))))

(deftest test-col
  (testing "Col function returns Column object with decoded name"
    (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
          opts {:write-key-fn (comp str/upper-case name)}]
      
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
  
  (testing "Col function works with different write-key-fn decodings"

    (testing "Lowercase decoding"
      (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
            lowercase-df {:dataframe mock-dataframe :write-key-fn (comp str/lower-case name)}
            result (df/col (mock-dataframe-wrapper mock-dataframe lowercase-df) :COLUMN_NAME)]
        (is (some? result))
        (assert/called-once? (:col mock-dataframe-spies))
        (assert/called-with? (:col mock-dataframe-spies) mock-dataframe "column_name")))
    
    (testing "String decoding (no case change)"
      (let [{:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe)
            opts {:write-key-fn str}
            result (df/col (mock-dataframe-wrapper mock-dataframe opts) :column-name)]
        (is (some? result))
        (assert/called-once? (:col mock-dataframe-spies))
                (assert/called-with? (:col mock-dataframe-spies) mock-dataframe ":column-name")))))

(deftest test-wrap-dataframe
  (testing "DataFrame wrapper supports map-like column access (feature 5)"
    (let [{:keys [mock-schema]} (mocks/mock-schema ["NAME" "SALARY" "AGE" "DEPARTMENT" "ID"])
          {:keys [mock-dataframe mock-dataframe-spies]} (mock-dataframe {:mock-schema mock-schema})
          test-df (#'df/wrap-dataframe mock-dataframe {:write-key-fn (comp str/upper-case name)
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

