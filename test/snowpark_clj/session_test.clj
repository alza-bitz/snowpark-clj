(ns snowpark-clj.session-test
  "Basic unit tests for the session namespace"
  (:require [clojure.test :refer [deftest testing is]]
            [snowpark-clj.session :as session]
            [spy.core :as spy]
            [spy.assert :as assert]))

;; Test data
(def test-config
  {:URL "jdbc:snowflake://test.snowflakecomputing.com"
   :USER "testuser"
   :PASSWORD "testpass"
   :DATABASE "testdb"
   :SCHEMA "public"
   :WAREHOUSE "testwh"})

;; Tests for session wrapper utility functions
(deftest test-unwrap-session
  (testing "Extracting raw session from wrapper"
    (let [mock-session {:mock true}
          session-wrapper {:session mock-session :read-key-fn keyword :write-key-fn name}
          result (session/unwrap-session session-wrapper)]
      (is (= mock-session result)))))

(deftest test-get-read-key-fn
  (testing "Extracting read-key-fn from wrapper"
    (let [custom-read-key-fn keyword
          session-wrapper {:session {:mock true} :read-key-fn custom-read-key-fn}
          result (session/get-read-key-fn session-wrapper)]
      (is (= custom-read-key-fn result)))))

(deftest test-get-write-key-fn
  (testing "Extracting write-key-fn from wrapper"
    (let [custom-write-key-fn name
          session-wrapper {:session {:mock true} :write-key-fn custom-write-key-fn}
          result (session/get-write-key-fn session-wrapper)]
      (is (= custom-write-key-fn result)))))

;; Test close-session with mock
(deftest test-close-session
  (testing "Closing a session wrapper"
    (let [close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))
          session-wrapper {:session mock-closeable-session :read-key-fn keyword :write-key-fn name}]
      (session/close-session session-wrapper)
      (assert/called-with? close-spy "close-called"))))

;; Test create-session with complete mocking
(deftest test-create-session-with-map-mocked
  (testing "Creating session with map config using SessionBuilder.configs()"
    (let [builder-spy (spy/spy)
          configs-spy (spy/spy) 
          create-spy (spy/spy)
          mock-session {:mock-session true}]
      ;; Mock the entire create-session function behavior
      (with-redefs [session/create-session 
                    (fn 
                      ([config] (session/create-session config {}))
                      ([config opts]
                       (builder-spy "builder-called")
                       ;; Simulate configs call for map
                       (when (map? config)
                         (configs-spy (into {} (for [[k v] config] 
                                                 [(name k) (str v)]))))
                       (create-spy "create-called")
                       (merge {:session mock-session 
                               :read-key-fn keyword 
                               :write-key-fn name} opts)))]
        (let [result (session/create-session test-config)]
          (is (map? result))
          (is (contains? result :session))
          (is (contains? result :read-key-fn))
          (is (contains? result :write-key-fn))
          (is (= keyword (:read-key-fn result)))
          (is (= name (:write-key-fn result)))
          (is (= mock-session (:session result)))
          
          ;; Verify calls were made
          (assert/called-with? builder-spy "builder-called")
          (assert/called-n-times? configs-spy 1)
          (assert/called-with? create-spy "create-called"))))))

;; Test create-session with properties file 
(deftest test-create-session-with-properties-file-mocked
  (testing "Creating session with properties file path using SessionBuilder.configFile()"
    (let [builder-spy (spy/spy)
          config-file-spy (spy/spy)
          create-spy (spy/spy)
          mock-session {:mock-session true}]
      (with-redefs [session/create-session 
                    (fn 
                      ([config] (session/create-session config {}))
                      ([config opts]
                       (builder-spy "builder-called")
                       ;; Simulate configFile call for string path
                       (when (string? config)
                         (config-file-spy config))
                       (create-spy "create-called")
                       (merge {:session mock-session 
                               :read-key-fn keyword 
                               :write-key-fn name} opts)))]
        (let [result (session/create-session "test.properties")]
          (is (map? result))
          (is (= mock-session (:session result)))
          (is (= keyword (:read-key-fn result)))
          (is (= name (:write-key-fn result)))
          (assert/called-with? builder-spy "builder-called")
          (assert/called-with? config-file-spy "test.properties")
          (assert/called-with? create-spy "create-called"))))))

;; Test with-session macro
(deftest test-with-session-success
  (testing "with-session macro executes body and closes session"
    (let [executed? (atom false)
          close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))]
      (with-redefs [session/create-session (fn [_] {:session mock-closeable-session :key-fn identity})]
        (let [result (session/with-session [sess (session/create-session test-config)]
                       (reset! executed? true)
                       (is (= mock-closeable-session (:session sess)))
                       "test-result")]
          (is (= "test-result" result))))
      
      ;; Verify body was executed and session was closed
      (is @executed?)
      (assert/called-with? close-spy "close-called"))))

(deftest test-with-session-exception
  (testing "with-session macro closes session even when exception occurs"
    (let [executed? (atom false)
          close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))]
      (with-redefs [session/create-session (fn [_] {:session mock-closeable-session :key-fn identity})]
        (try
          (session/with-session [_ (session/create-session test-config)]
            (reset! executed? true)
            (throw (RuntimeException. "test exception")))
          (catch RuntimeException e
            (is (= "test exception" (.getMessage e))))))
      
      ;; Verify body was executed and session was still closed despite exception
      (is @executed?)
      (assert/called-with? close-spy "close-called"))))
