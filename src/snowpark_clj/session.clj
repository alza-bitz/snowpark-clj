(ns snowpark-clj.session
  "The internal API for Snowpark session functions."
  (:require
   [aero.core :as aero]
   [clojure.string :as str]
   [clojure.test.check.generators :as gen]
   [malli.core :as m]
   [malli.error :as me]
   [malli.registry :as mr]
   [mask.aero]
   [mask.core :as mask])
  (:import
   [com.snowflake.snowpark_java Session]))

(comment
  (mask/mask? "x"))

(def mask-schema
  [:fn {:error/message "must be a non-empty string tagged with #mask"
        :gen/gen (gen/fmap mask/mask gen/string-alphanumeric)}
   #(and (mask/mask? %)
         (string? (mask/unmask %))
         (not-empty (mask/unmask %)))])

(mr/set-default-registry!
 (mr/composite-registry
  (m/default-schemas)
  {:mask mask-schema}))

(def config-schema
  [:map {:closed true}
   [:url [:string {:min 1}]]
   [:user [:string {:min 1}]]
   [:password :mask]
   [:role {:optional true} [:string {:min 1}]]
   [:warehouse {:optional true} [:string {:min 1}]]
   [:db {:optional true} [:string {:min 1}]]
   [:schema {:optional true} [:string {:min 1}]]
   [:insecureMode {:optional true} [:boolean]]])

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
  "Create a session from a map or edn file config, with optional column name encoding and decoding functions.
      
   Args:
   - config: Map or the path of an edn file, either must conform to config-schema
   - opts: Map that can include:
     - :read-key-fn - function to encode column names on dataset read operations
     - :write-key-fn - function to decode column names on dataset write operations
   
   Returns: A session wrapper with the session options"
  ([config]
   (create-session config {}))
  ([config {:keys [read-key-fn write-key-fn] :or {read-key-fn (:read-key-fn default-opts)
                                                  write-key-fn (:write-key-fn default-opts)} :as opts}]
   (let [loaded-config (if (string? config)
                         (aero/read-config config)
                         config)
         _ (when-not (m/validate config-schema loaded-config)
             (let [explanation (m/explain config-schema loaded-config)]
               (throw (ex-info (str "Invalid config: " (me/humanize explanation))
                               {:config loaded-config
                                :explanation explanation}))))
         config-map (into {} (for [[k v] loaded-config]
                               [(name k) ((comp str mask/unmask) v)]))
         builder (create-session-builder)
         configured-builder (.configs builder config-map)
         session (.create configured-builder)]
     (wrap-session session (merge {:read-key-fn read-key-fn
                                   :write-key-fn write-key-fn}
                                  opts)))))

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
