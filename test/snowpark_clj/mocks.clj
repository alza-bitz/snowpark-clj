(ns snowpark-clj.mocks 
  (:refer-clojure :exclude [filter sort count])
  (:require
   [spy.protocol :as protocol]) 
  (:import
   [com.snowflake.snowpark_java Functions]))

(defprotocol MockStructType
  (names [_] "Mock StructType.names method"))

(defn mock-schema
  [field-names]
  (let [mock (protocol/mock MockStructType
                            (names [_] (into-array String field-names)))]
    {:mock-schema mock
     :mock-schema-spies (protocol/spies mock)}))

(defprotocol MockDataFrame
  (col [this col-name] "Mock DataFrame.col method")
  (filter [this condition] "Mock DataFrame.filter method")
  (select [this col-array] "Mock DataFrame.select method")
  (limit [this n] "Mock DataFrame.limit method")
  (sort [this col-array] "Mock DataFrame.sort method")
  (groupBy [this col-array] "Mock DataFrame.groupBy method")
  (join [this other-df join-expr join-type] "Mock DataFrame.join method")
  (collect [this] "Mock DataFrame.collect method")
  (show [this n] "Mock DataFrame.show method")
  (count [this] "Mock DataFrame.count method")
  (schema [this] "Mock DataFrame.schema method")
  (write [this] "Mock DataFrame.write method"))

(defn mock-dataframe
  [& {:keys [lazy-chain? mock-writer mock-schema]
      :or {lazy-chain? false mock-writer "mock-writer" mock-schema "mock-schema"}}]
  (let [mock (protocol/mock MockDataFrame
                            (col [_ _] (Functions/col "col"))
                            (filter [this _] (if lazy-chain? this "filter"))
                            (select [this _] (if lazy-chain? this "select"))
                            (limit [this _] (if lazy-chain? this "limit"))
                            (sort [this _] (if lazy-chain? this "sort"))
                            (groupBy [this _] (if lazy-chain? this "groupBy"))
                            (join [this _ _ _] (if lazy-chain? this "join"))
                            (collect [_] "mock-rows")
                            (show [_ _] nil)
                            (count [_] "mock-count")
                            (schema [_] mock-schema)
                            (write [_] mock-writer))]
    {:mock-dataframe mock
     :mock-dataframe-spies (protocol/spies mock)}))
