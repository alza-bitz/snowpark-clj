(ns snowpark-clj.session-test
  "Basic unit tests for the session namespace"
  (:require
   [aero.core :as aero]
   [clojure.test :refer [deftest is testing]]
   [mask.core :as mask]
   [snowpark-clj.session :as session]
   [snowpark-clj.wrapper :as wrapper]
   [spy.assert :as assert]
   [spy.core :as spy]
   [spy.protocol :as protocol]))

(def test-config
  {:url "jdbc:snowflake://test.snowflakecomputing.com"
   :user "testuser"
   :password (mask/mask "testpass")})

(defprotocol MockSessionBuilder
  (configs [this config-map] "Mock SessionBuilder.configs method")
  (configFile [this file-path] "Mock SessionBuilder.configFile method")
  (create [this] "Mock SessionBuilder.create method"))

(defn- mock-builder []
  (let [mock-configured (protocol/mock MockSessionBuilder
                                       (configs [_ _] nil)
                                       (configFile [_ _] nil)
                                       (create [_] "mock-session"))
        mock (protocol/mock MockSessionBuilder
                            (configs [_ _] mock-configured)
                            (configFile [_ _] mock-configured)
                            (create [_] nil))]
    {:mock-builder mock
     :mock-builder-spies (protocol/spies mock)
     :mock-builder-configured mock-configured
     :mock-builder-configured-spies (protocol/spies mock-configured)}))

(deftest test-close-session
  (testing "Close session in wrapper"
    (let [close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))
          mock-session-wrapper (protocol/mock wrapper/IWrappedSessionOptions
                                 (unwrap [_] mock-closeable-session))
          mock-session-wrapper-spies (protocol/spies mock-session-wrapper)]
          
      (session/close-session mock-session-wrapper)
      (assert/called-once? (:unwrap mock-session-wrapper-spies))
      (assert/called-with? close-spy "close-called"))))

(deftest test-create-session
  (testing "Create session from config as map"
    (let [{:keys [mock-builder mock-builder-spies
                  mock-builder-configured-spies]} (mock-builder)
          expected-config-map {"url" "jdbc:snowflake://test.snowflakecomputing.com"
                               "user" "testuser"
                               "password" "testpass"}]

      (with-redefs [session/create-session-builder (fn [] mock-builder)]
        (let [result (session/create-session test-config)]

          ;; Verify the result is properly wrapped
          (is (wrapper/wrapper? result))
          ;; Check that the functions work correctly, not that they equal specific functions
          (is (= :test ((wrapper/unwrap-option result :read-key-fn) "TEST")))
          (is (= "TEST" ((wrapper/unwrap-option result :write-key-fn) :test)))
          (is (= "mock-session" (wrapper/unwrap result)))

          ;; Verify the builder methods were called correctly
          (assert/called-once? (:configs mock-builder-spies))
          (assert/called-with? (:configs mock-builder-spies) mock-builder expected-config-map)
          (assert/called-once? (:create mock-builder-configured-spies))))))

  (testing "Create session from config as edn file"
    (let [{:keys [mock-builder mock-builder-spies
                  mock-builder-configured-spies]} (mock-builder)
          mock-config {:url "jdbc:snowflake://test.snowflakecomputing.com"
                       :user "testuser"
                       :password (mask/mask "testpass")}
          expected-config-map {"url" "jdbc:snowflake://test.snowflakecomputing.com"
                               "user" "testuser"
                               "password" "testpass"}]

      (with-redefs [session/create-session-builder (fn [] mock-builder)
                    aero/read-config (spy/stub mock-config)]
        (let [result (session/create-session "test-config.edn")]

          ;; Verify the result is properly wrapped
          (is (wrapper/wrapper? result))
          ;; Check that the functions work correctly, not that they equal specific functions
          (is (= :test ((wrapper/unwrap-option result :read-key-fn) "TEST")))
          (is (= "TEST" ((wrapper/unwrap-option result :write-key-fn) :test)))
          (is (= "mock-session" (wrapper/unwrap result)))

          (assert/called-once? aero/read-config)
          (assert/called-with? aero/read-config "test-config.edn")

          ;; Verify the builder methods were called correctly
          (assert/called-once? (:configs mock-builder-spies))
          (assert/called-with? (:configs mock-builder-spies) mock-builder expected-config-map)
          (assert/called-once? (:create mock-builder-configured-spies))))))

  (testing "Create session with invalid config throws exception"
    (let [{:keys [mock-builder mock-builder-spies
                  mock-builder-configured-spies]} (mock-builder)
          invalid-config nil]
      (with-redefs [session/create-session-builder (fn [] mock-builder)]
        (is (thrown-with-msg? Exception #"Invalid config"
                              (session/create-session invalid-config)))
        ;; Verify the builder methods were not called
        (assert/not-called? (:configs mock-builder-spies))
        (assert/not-called? (:create mock-builder-configured-spies))))))

(deftest test-with-session
  (testing "With session macro executes body and closes session"
    (let [executed? (atom false)
          close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))
          mock-session-wrapper (protocol/mock wrapper/IWrappedSessionOptions
                                              (unwrap [_] mock-closeable-session))
          mock-session-wrapper-spies (protocol/spies mock-session-wrapper)]
      (with-redefs [session/create-session (fn [_] mock-session-wrapper)]
        (let [result (session/with-session [_ (session/create-session test-config)]
                       (reset! executed? true)
                       "test-result")]
          (is (= "test-result" result))))

      ;; Verify body was executed and session was closed
      (is @executed?)
      (assert/called-once? (:unwrap mock-session-wrapper-spies))
      (assert/called-with? close-spy "close-called")))

  (testing "With session macro closes session even when exception occurs"
    (let [executed? (atom false)
          close-spy (spy/spy)
          mock-closeable-session (reify java.io.Closeable
                                   (close [_] (close-spy "close-called")))
          mock-session-wrapper (protocol/mock wrapper/IWrappedSessionOptions
                                              (unwrap [_] mock-closeable-session))
          mock-session-wrapper-spies (protocol/spies mock-session-wrapper)]
      (with-redefs [session/create-session (fn [_] mock-session-wrapper)]
        (try
          (session/with-session [_ (session/create-session test-config)]
            (reset! executed? true)
            (throw (RuntimeException. "test exception")))
          (catch RuntimeException e
            (is (= "test exception" (.getMessage e))))))

      ;; Verify body was executed and session was still closed despite exception
      (is @executed?)
      (assert/called-once? (:unwrap mock-session-wrapper-spies))
      (assert/called-with? close-spy "close-called")))
  )
