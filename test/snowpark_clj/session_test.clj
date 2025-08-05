(ns snowpark-clj.session-test
  "Basic unit tests for the session namespace"
  (:require [clojure.test :refer [deftest testing is]]
            [snowpark-clj.session :as session]
            [spy.core :as spy]
            [spy.assert :as assert]
            [spy.protocol :as protocol]))

;; Test data
(def test-config
  {:URL "jdbc:snowflake://test.snowflakecomputing.com"
   :USER "testuser"
   :PASSWORD "testpass"
   :DATABASE "testdb"
   :SCHEMA "public"
   :WAREHOUSE "testwh"})

(defprotocol MockSessionBuilder
  (configs [this config-map] "Mock SessionBuilder.configs method")
  (configFile [this file-path] "Mock SessionBuilder.configFile method")
  (create [this] "Mock SessionBuilder.create method"))

(defprotocol MockSession
  (close [this] "Mock Session.close method"))

(defn- mock-builder [mock-session-builder-configured]
  (let [mock (protocol/mock MockSessionBuilder
                            (configs [_ _] mock-session-builder-configured)
                            (configFile [_ _] mock-session-builder-configured))
        spies (protocol/spies mock)]
    {:mock-builder mock
     :mock-builder-spies spies}))

(defn- mock-builder-configured [mock-session]
  (let [mock (protocol/mock MockSessionBuilder
                            (create [_] mock-session))
        spies (protocol/spies mock)]
    {:mock-builder-configured mock
     :mock-builder-configured-spies spies}))

(defn- mock-session []
  (let [mock (protocol/mock MockSession)
        spies (protocol/spies mock)]
    {:mock-session mock
     :mock-session-spies spies}))

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

(deftest test-close-session
  (testing "Closing a session wrapper"
    (let [close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))
          session-wrapper {:session mock-closeable-session :read-key-fn keyword :write-key-fn name}]
      (session/close-session session-wrapper)
      (assert/called-with? close-spy "close-called"))))

(deftest test-create-session
  (testing "Creating session with map config using SessionBuilder.configs()"
    (let [{:keys [mock-session]} (mock-session)
          {:keys [mock-builder-configured mock-builder-configured-spies]} (mock-builder-configured mock-session)
          {:keys [mock-builder mock-builder-spies]} (mock-builder mock-builder-configured)
          expected-config-map {"URL" "jdbc:snowflake://test.snowflakecomputing.com"
                               "USER" "testuser"
                               "PASSWORD" "testpass"
                               "DATABASE" "testdb"
                               "SCHEMA" "public"
                               "WAREHOUSE" "testwh"}]

      (with-redefs [session/create-session-builder (fn [] mock-builder)]
        (let [result (session/create-session test-config)]

          ;; Verify the result is properly wrapped
          (is (map? result))
          (is (contains? result :session))
          (is (contains? result :read-key-fn))
          (is (contains? result :write-key-fn))
          ;; Check that the functions work correctly, not that they equal specific functions
          (is (= :test ((:read-key-fn result) "TEST")))
          (is (= "TEST" ((:write-key-fn result) :test)))
          (is (= mock-session (:session result)))

          ;; Verify the builder methods were called correctly
          (assert/called-once? (:configs mock-builder-spies))
          (assert/called-with? (:configs mock-builder-spies) mock-builder expected-config-map)
          (assert/called-once? (:create mock-builder-configured-spies))))))

  (testing "Creating session with properties file path using SessionBuilder.configFile()"
    (let [{:keys [mock-session]} (mock-session)
          {:keys [mock-builder-configured mock-builder-configured-spies]} (mock-builder-configured mock-session)
          {:keys [mock-builder mock-builder-spies]} (mock-builder mock-builder-configured)]

      (with-redefs [session/create-session-builder (fn [] mock-builder)]
        (let [result (session/create-session "test.properties")]

          ;; Verify the result is properly wrapped
          (is (map? result))
          (is (contains? result :session))
          (is (contains? result :read-key-fn))
          (is (contains? result :write-key-fn))
          ;; Check that the functions work correctly, not that they equal specific functions
          (is (= :test ((:read-key-fn result) "TEST")))
          (is (= "TEST" ((:write-key-fn result) :test)))
          (is (= mock-session (:session result)))

          ;; Verify the builder methods were called correctly
          (assert/called-once? (:configFile mock-builder-spies))
          (assert/called-with? (:configFile mock-builder-spies) mock-builder "test.properties")
          (assert/called-once? (:create mock-builder-configured-spies)))))))

(deftest test-with-session
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
      (assert/called-with? close-spy "close-called")))

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
