(ns snowpark-clj.session
  "Session management and connection utilities"
  (:require [clojure.string :as str])
  (:import [com.snowflake.snowpark_java Session]))

(defn create-session-builder
  "Create a new SessionBuilder instance"
  []
  (Session/builder))

(defn create-session
  "Create a Snowpark session with optional transformation functions.
   
   Two ways to create a session:
   1. With a map of keywords corresponding to Snowpark properties (uses SessionBuilder.configs())
   2. With a path to a properties file (uses SessionBuilder.configFile())
   
   Args:
   - config: Map with connection parameters (keywords) or path to properties file (string)
   - opts: Optional map that can include:
     - :read-key-fn - function to transform column names on dataset read operations (default: keyword)
     - :write-key-fn - function to transform column names on dataset write operations (default: name)
   
   Returns: Map with :session (Snowpark Session) and the opts"
  ([config]
   (create-session config {}))
  ([config opts]
   (let [builder (create-session-builder)
         configured-builder (if (string? config)
                              ;; Use configFile() for properties file path
                              (.configFile builder config)
                              ;; Use configs() for map of properties
                              (let [config-map (into {} (for [[k v] config] 
                                                         [(name k) (str v)]))]
                                (.configs builder config-map)))
         session (.create configured-builder)
         final-opts (merge {:read-key-fn (comp keyword str/lower-case)
                            :write-key-fn (comp str/upper-case name)}
                           opts)]
     (merge {:session session} final-opts))))

(defn close-session
  "Close a Snowpark session"
  [session-wrapper]
  (.close (:session session-wrapper)))

(defn unwrap-session
  "Extract the raw Snowpark session from a session wrapper"
  [session-wrapper]
  (:session session-wrapper))

(defn get-read-key-fn
  "Extract the read-key-fn from a session wrapper"
  [session-wrapper]
  (:read-key-fn session-wrapper))

(defn get-write-key-fn
  "Extract the write-key-fn from a session wrapper"
  [session-wrapper]
  (:write-key-fn session-wrapper))

(defmacro with-session
  "Execute body with a Snowpark session, automatically closing when done.
   
   Usage:
   (with-session [session (create-session config key-fn)]
     (do-something-with session))"
  [[binding session-expr] & body]
  `(let [~binding ~session-expr]
     (try
       ~@body
       (finally 
         (close-session ~binding)))))
