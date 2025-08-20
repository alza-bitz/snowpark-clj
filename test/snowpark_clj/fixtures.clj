(ns snowpark-clj.fixtures 
  (:require
   [snowpark-clj.dataframe :as df]
   [snowpark-clj.session :as session]))

(def ^:dynamic *session* nil)

(defn session-fixture
  "Returns a fixture that creates a new session from `config` and binds it as `*session*`
   for the scope of the test. Also ensures that `table-name` is dropped afterwards."
  [config table-name]
  (fn [test-fn]
    (with-open [session (session/create-session config)]
      (try
        (binding [*session* session]
          (test-fn))
        (finally
          (try
            (df/sql session (str "DROP TABLE IF EXISTS " table-name))
            (catch Exception e
              (println "Warning: Could not drop table during cleanup:" (.getMessage e)))))))))