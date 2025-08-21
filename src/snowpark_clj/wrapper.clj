(ns snowpark-clj.wrapper)

(defprotocol IWrappedSessionOptions
  "Protocol for accessing Snowpark objects that are wrapped with session options for convenience"
  (unwrap [this] "Get the Snowpark object from a wrapper")
  (unwrap-option [this option-key] "Get a session option from a wrapper")
  (unwrap-options [this] "Get all session options from a wrapper"))

(defn wrapper?
  "Returns true if obj is a Snowpark object wrapper"
  [obj]
  (satisfies? IWrappedSessionOptions obj))

(defn wrap-session
  "Wrap a Snowpark Session with session options and java.io.Closeable"
  [session opts]
  (let [base-map (merge {:session session} opts)]
    (reify

      clojure.lang.IHashEq
      (hasheq [_]
        (.hasheq base-map))

      Object
      (hashCode [_]
        (.hashCode base-map))
      (equals [this o]
        (.equiv this o))
      (toString [_]
        (.toString base-map))

      IWrappedSessionOptions
      (unwrap [_] (:session base-map))
      (unwrap-option [_ option-key] (get base-map option-key))
      (unwrap-options [_] (dissoc base-map :session))
      
      java.io.Closeable
      (close [_]
        (.close (:session base-map))))))

(defn wrap-dataframe
  "Wrap a Snowpark DataFrame with session options and map-like column access"
  [df opts]
  (let [base-map (merge {:dataframe df} opts)]

    (reify
      clojure.lang.ILookup
      (valAt [this k]
        (.valAt this k nil))
      (valAt [_ k not-found]
        (let [key->col-fn (:key->col-fn base-map)
              raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (set (.names schema))]
          (if (contains? field-names (key->col-fn k))
            (.col raw-df (key->col-fn k))
            not-found)))

      clojure.lang.IFn
      (invoke [this k]
        (.valAt this k))
      (invoke [this k not-found]
        (.valAt this k not-found))

      clojure.lang.IPersistentMap
      (assoc [_ _ _]
        (throw (UnsupportedOperationException. "Cannot assoc on dataframe wrapper")))
      (without [_ _]
        (throw (UnsupportedOperationException. "Cannot dissoc on dataframe wrapper")))
      (iterator [this]
        ;; Support keys and vals operations
        (.iterator (.seq this)))

      clojure.lang.Associative
      (containsKey [_ k]
        (let [key->col-fn (:key->col-fn base-map)
              raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (set (.names schema))]
          (contains? field-names (key->col-fn k))))
      (entryAt [this k]
        (when (.containsKey this k)
          (clojure.lang.MapEntry. k (.valAt this k))))

      clojure.lang.IPersistentCollection
      (count [_]
        ;; Return the number of fields in the DataFrame schema
        (let [raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (.names schema)]
          (alength field-names)))
      (empty [_]
        {})
      (equiv [_ o]
        (and (map? o) (= base-map o)))

      clojure.lang.Seqable
      (seq [_]
        ;; Return a seq of MapEntry objects, each containing the transformed field name and column object
        (let [raw-df (:dataframe base-map)
              schema (.schema raw-df)
              field-names (.names schema)
              col->key-fn (:col->key-fn base-map)]
          (map (fn [field-name]
                 (let [column-obj (.col raw-df field-name)]
                   (clojure.lang.MapEntry. (col->key-fn field-name) column-obj)))
               field-names)))

      ;; Additional map-like operations for DataFrame columns
      clojure.lang.IKVReduce
      (kvreduce [this f init]
        (reduce (fn [acc [k v]] (f acc k v)) init (.seq this)))

      clojure.lang.IHashEq
      (hasheq [_]
        (.hasheq base-map))

      Object
      (hashCode [_]
        (.hashCode base-map))
      (equals [this o]
        (.equiv this o))
      (toString [_]
        (.toString base-map))

      IWrappedSessionOptions
      (unwrap [_] (:dataframe base-map))
      (unwrap-option [_ option-key] (get base-map option-key))
      (unwrap-options [_] (dissoc base-map :dataframe)))))

(defn wrap-grouped
  "Wrap a Snowpark RelationalGroupedDataFrame with session options"
  [session opts]
  (let [base-map (merge {:grouped session} opts)]
    (reify

      clojure.lang.IHashEq
      (hasheq [_]
        (.hasheq base-map))

      Object
      (hashCode [_]
        (.hashCode base-map))
      (equals [this o]
        (.equiv this o))
      (toString [_]
        (.toString base-map))

      IWrappedSessionOptions
      (unwrap [_] (:grouped base-map))
      (unwrap-option [_ option-key] (get base-map option-key))
      (unwrap-options [_] (dissoc base-map :grouped)))))