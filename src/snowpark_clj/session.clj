(ns snowpark-clj.session
  "The internal API for Snowpark session functions."
  (:require
   [clojure.string :as str]
   [malli.core :as m]
   [malli.error :as me]
   [mask.core :as mask]
   [snowpark-clj.column :as col]
   [snowpark-clj.config :as config]
   [snowpark-clj.wrapper :as wrapper])
  (:import
   [com.snowflake.snowpark_java Session]))

(defn create-session-builder
  "Create a new SessionBuilder instance"
  []
  (Session/builder))

(def default-opts
  {:col->key-fn (comp keyword str/lower-case col/quoted)
   :key->col-fn (comp str/upper-case name)})

(defn create-session
  "Create a session from a map or edn file config, with optional key<->column transformation functions.
      
   Args:
   - config: Map or the path of an edn file, either must conform to `snowpark-clj.config/config-schema`
   - opts: Map that can include:
     - :col->key-fn - function to transform column names on dataset read operations
     - :key->col-fn - function to transform map keys on dataset write operations
   
   Returns: A session wrapper with the session options"
  ([config]
   (create-session config {}))
  ([config {:keys [col->key-fn key->col-fn] :or {col->key-fn (:col->key-fn default-opts)
                                                 key->col-fn (:key->col-fn default-opts)} :as opts}]
   (let [loaded-config (if (string? config)
                         (config/read-config config)
                         config)
         _ (when-not (m/validate config/config-schema loaded-config)
             (let [explanation (m/explain config/config-schema loaded-config)]
               (throw (ex-info (str "Invalid config: " (me/humanize explanation))
                               {:config loaded-config
                                :explanation explanation}))))
         config-map (into {} (for [[k v] loaded-config]
                               [(name k) ((comp str mask/unmask) v)]))
         builder (create-session-builder)
         configured-builder (.configs builder config-map)
         session (.create configured-builder)]
     (wrapper/wrap-session session (merge {:col->key-fn col->key-fn
                                           :key->col-fn key->col-fn}
                                          opts)))))

(defn close-session
  "Close a session"
  [session-wrapper]
  (.close (wrapper/unwrap session-wrapper)))
