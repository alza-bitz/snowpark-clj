(ns snowpark-clj.session
  "The internal API for Snowpark session functions."
  (:require [clojure.string :as str])
  (:import [com.snowflake.snowpark_java Session]))

(defn unwrap-session
  "Extract the Snowpark Session from a session wrapper"
  [session-wrapper]
  (:session session-wrapper))

(defn unwrap-read-key-fn
  "Extract the read-key-fn from a session wrapper"
  [session-wrapper]
  (:read-key-fn session-wrapper))

(defn unwrap-write-key-fn
  "Extract the write-key-fn from a session wrapper"
  [session-wrapper]
  (:write-key-fn session-wrapper))

(defn unwrap-options
  "Extract all options from a session wrapper"
  [session]
  (dissoc session :session))

(defn- wrap-session
  "Wrap a Snowpark Session with session options"
  [session opts]
  (merge {:session session} opts))

(defn create-session-builder
  "Create a new SessionBuilder instance"
  []
  (Session/builder))

(def default-opts
  {:read-key-fn (comp keyword str/lower-case)
   :write-key-fn (comp str/upper-case name)})

(defn create-session
  "Create a session with optional column name encoding and decoding functions.
   
   Two ways to create a session:
   1. With a map of keywords corresponding to Snowpark properties (uses SessionBuilder.configs())
   2. With a path to a properties file (uses SessionBuilder.configFile())
   
   Args:
   - config: Map with connection parameters (keywords) or path to properties file (string)
   - opts: Optional map that can include:
     - :read-key-fn - function to encode column names on dataset read operations
     - :write-key-fn - function to decode column names on dataset write operations
   
   Returns: A session wrapper with the session options"
  [config & {:keys [read-key-fn write-key-fn] :or {read-key-fn (:read-key-fn default-opts)
                                                   write-key-fn (:write-key-fn default-opts)} :as opts}]
  (let [builder (create-session-builder)
        configured-builder (if (string? config)
                             ;; Use configFile() for properties file path
                             (.configFile builder config)
                             ;; Use configs() for map of properties
                             (let [config-map (into {} (for [[k v] config]
                                                         [(name k) (str v)]))]
                               (.configs builder config-map)))
        session (.create configured-builder)]
    (wrap-session session (merge {:read-key-fn read-key-fn
                                  :write-key-fn write-key-fn}
                                 opts))))

(defn close-session
  "Close a session"
  [session-wrapper]
  (.close (:session session-wrapper)))

(defmacro with-session
  "Execute body with a session, automatically closing when done.
   
   Usage:
   (with-session [session (create-session config key-fn)]
     (do-something-with session))"
  [[binding session-expr] & body]
  `(let [~binding ~session-expr]
     (try
       ~@body
       (finally
         (close-session ~binding)))))
