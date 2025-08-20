(ns snowpark-clj.column-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.generators :as gen]
   [snowpark-clj.column :as column]))

(deftest test-parse-quoted
  (testing "with unquoted column name"
    (let [result (column/parse-quoted "DEPT")]
      (is (= "DEPT" (:col-name result)))
      (is (= "DEPT" (:unquoted result)))
      (is (nil? (:quoted result)))))
  
  (testing "with quoted aggregation column name"
    (let [result (column/parse-quoted "\"COUNT(DEPT)\"")]
      (is (= "\"COUNT(DEPT)\"" (:col-name result)))
      (is (nil? (:unquoted result)))
      (is (= "COUNT(DEPT)" (:quoted result)))))
  
  (testing "with quoted non-aggregation column name"
    (let [result (column/parse-quoted "\"DEPT\"")]
      (is (= "\"DEPT\"" (:col-name result)))
      (is (nil? (:unquoted result)))
      (is (= "DEPT" (:quoted result)))))
  
  (testing "with nil input"
    (is (nil? (column/parse-quoted nil))))
  
  (testing "with empty string"
    (let [result (column/parse-quoted "")]
      (is (= "" (:col-name result)))
      (is (= "" (:unquoted result)))
      (is (nil? (:quoted result))))))

(deftest test-quoted
  (testing "with unquoted column name"
    (is (= "DEPT" (column/quoted "DEPT")))
    (is (= "EMPLOYEE_ID" (column/quoted "EMPLOYEE_ID"))))
  
  (testing "with quoted aggregation column names"
    (is (= "COUNT-DEPT" (column/quoted "\"COUNT(DEPT)\"")))
    (is (= "AVG-SALARY" (column/quoted "\"AVG(SALARY)\"")))
    (is (= "SUM-AMOUNT" (column/quoted "\"SUM(AMOUNT)\"")))
    (is (= "MAX-DATE" (column/quoted "\"MAX(DATE)\"")))
    (is (= "MIN-ID" (column/quoted "\"MIN(ID)\""))))
  
  (testing "with quoted non-aggregation column names should throw exception"
    (is (thrown-with-msg? 
         clojure.lang.ExceptionInfo 
         #"Quoted column names are not supported"
         (column/quoted "\"DEPT\"")))
    (is (thrown-with-msg? 
         clojure.lang.ExceptionInfo 
         #"Quoted column names are not supported"
         (column/quoted "\"EMPLOYEE_NAME\""))))
  
  (testing "with nil input"
    (is (nil? (column/quoted nil))))
  
  (testing "with malformed quoted aggregation"
    (is (thrown-with-msg? 
         clojure.lang.ExceptionInfo 
         #"Quoted column names are not supported"
         (column/quoted "\"COUNT\"")))
    (is (thrown-with-msg? 
         clojure.lang.ExceptionInfo 
         #"Quoted column names are not supported"
         (column/quoted "\"COUNT()\""))))
  
  (testing "edge cases"
    (is (= "" (column/quoted "")))
    (is (= "SIMPLE" (column/quoted "SIMPLE")))))

(defspec parse-quoted-unquoted-property 100
  (prop/for-all [col-name (gen/such-that seq gen/string-alphanumeric)]
    (let [result (column/parse-quoted col-name)]
      (and (= col-name (:col-name result))
           (= col-name (:unquoted result))
           (nil? (:quoted result))))))

(defspec quoted-unquoted-passthrough-property 100
  (prop/for-all [col-name (gen/such-that seq gen/string-alphanumeric)]
    (= col-name (column/quoted col-name))))

(defspec quoted-aggregation-transformation-property 100
  (prop/for-all [agg-fn (gen/elements ["COUNT" "AVG" "SUM" "MAX" "MIN" "STDDEV" "VARIANCE"])
                 col-name (gen/such-that seq gen/string-alphanumeric)]
    (let [quoted-col (str "\"" agg-fn "(" col-name ")\"")
          expected (str agg-fn "-" col-name)
          result (column/quoted quoted-col)]
      (= expected result))))

(defspec quoted-non-aggregation-exception-property 100
  (prop/for-all [col-name (gen/such-that seq gen/string-alphanumeric)]
    (let [quoted-col (str "\"" col-name "\"")]
      (try
        (column/quoted quoted-col)
        false ; Should not reach here
        (catch clojure.lang.ExceptionInfo e
          (re-find #"Quoted column names are not supported" (ex-message e)))))))
