(ns snowpark-clj.mocks 
  (:require
   [spy.protocol :as protocol]))

(defprotocol MockStructType
  (names [_] "Mock StructType.names method"))

(defn mock-schema
  [field-names]
  (let [mock (protocol/mock MockStructType
                            (names [_] (into-array String field-names)))]
    {:mock-schema mock
     :mock-schema-spies (protocol/spies mock)}))