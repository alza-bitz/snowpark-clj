(ns snowpark-clj.session-integration-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [mask.core :as mask]
   [snowpark-clj.session :as session]
   [snowpark-clj.wrapper :as wrapper]
   [snowpark-clj.config :as config]))

(def test-config
  {:url "https://vkvupnf-yyb56283.snowflakecomputing.com"
   :user "ALZA"
   :role "SNOWFLAKE_LEARNING_ROLE"
   :warehouse "SNOWFLAKE_LEARNING_WH"
   :db "SNOWFLAKE_LEARNING_DB"
   :schema "SNOWPARK_CLJ_TEST_SCHEMA"
   :insecureMode true})

(defn- quote-string [s]
  (str "\"" s "\""))

(defn- assoc-password [config]
  (let [password (System/getenv "SNOWFLAKE_PASSWORD")]
    (assoc config :password (mask/mask password))))

(deftest test-create-session

  (testing "Create session from config as map"
    (let [test-config-with-password (assoc-password test-config)]
      (with-open [result (session/create-session test-config-with-password)]
        (is (wrapper/wrapper? result))
        ;; Test session options
        (is (fn? (wrapper/unwrap-option result :read-key-fn)))  ; Test it's a function rather than specific function
        (is (fn? (wrapper/unwrap-option result :write-key-fn))) ; Test it's a function rather than specific function

        (is (string? (.getSessionInfo (wrapper/unwrap result))))
        (is (= (quote-string (:db test-config-with-password))
               (.get (.getDefaultDatabase (wrapper/unwrap result)))))
        (is (= (quote-string (:schema test-config-with-password))
               (.get (.getDefaultSchema (wrapper/unwrap result))))))))

  (testing "Create session from config as map with custom options"
    (with-open [result (session/create-session (assoc-password test-config)
                                               {:read-key-fn identity
                                                :write-key-fn str/upper-case})]
      (is (wrapper/wrapper? result))
      (is (= identity (wrapper/unwrap-option result :read-key-fn)))
      (is (= str/upper-case (wrapper/unwrap-option result :write-key-fn)))))

  (testing "Create session from config as map with incorrect url"
    (is (thrown? Exception (session/create-session (assoc test-config :url "http://haha.com")))))

  (comment "Disabled because repeated runs will result in the account being locked!!!"
           (testing "Create session from map with incorrect password"
             (let [config-with-incorrect-password (assoc test-config :password "incorrect")]
               (is (thrown? net.snowflake.client.jdbc.SnowflakeSQLException
                            (session/create-session config-with-incorrect-password))))))

  (testing "Create session from config as edn file"
    (with-open [result (session/create-session "integration/snowflake.edn")]
      (is (string? (.getSessionInfo (wrapper/unwrap result))))
      (let [loaded-config (config/read-config "integration/snowflake.edn")]
        (is (= (quote-string (:db loaded-config))
               (.get (.getDefaultDatabase (wrapper/unwrap result)))))
        (is (= (quote-string (:schema loaded-config))
               (.get (.getDefaultSchema (wrapper/unwrap result)))))))))

