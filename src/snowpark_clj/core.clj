(ns snowpark-clj.core
  "Main API for Snowpark Clojure wrapper"
  (:require [potemkin :refer [import-vars]]
            [snowpark-clj.column :as col]
            [snowpark-clj.dataframe :as df]
            [snowpark-clj.functions :as fn]
            [snowpark-clj.schema]
            [snowpark-clj.session])
  (:refer-clojure :exclude [filter sort group-by take
                            and not or abs count max min]))

;; Re-export functions from other layers with preserved docstrings and arglists
(import-vars
  [snowpark-clj.column 
   gt
   lt
   eq
   geq
   leq
   neq]

  [snowpark-clj.session
   create-session
   close-session]
 
  [snowpark-clj.schema
   malli-schema->snowpark-schema]

  [snowpark-clj.dataframe
   create-dataframe
   table
   sql
   select
   where
   limit
   agg
   join
   collect
   row-count
   show 
   save-as-table
   col
   schema]
  
  [snowpark-clj.functions 
   lit
   avg
   sum
   upper
   lower])

;; Create clean aliases for prefixed functions that conflict with clojure.core
;; These preserve the original metadata (docstrings, arglists) from the prefixed versions
(def ^{:arglists (:arglists (meta #'df/df-filter))
       :doc (:doc (meta #'df/df-filter))}
  filter df/df-filter)

(def ^{:arglists (:arglists (meta #'df/df-sort))
       :doc (:doc (meta #'df/df-sort))}
  sort df/df-sort)

(def ^{:arglists (:arglists (meta #'df/df-group-by))
       :doc (:doc (meta #'df/df-group-by))}
  group-by df/df-group-by)

(def ^{:arglists (:arglists (meta #'df/df-take))
       :doc (:doc (meta #'df/df-take))}
  take df/df-take)

;; Same as above but done declaratively / data-driven style
(let [alias-mappings [['col/and-fn 'and]
                      ['col/not-fn 'not]
                      ['col/or-fn 'or]
                      ['fn/abs-fn 'abs]
                      ['fn/count-fn 'count]
                      ['fn/max-fn 'max]
                      ['fn/min-fn 'min]]]
  (doseq [[source-fn target-name] alias-mappings]
    (let [source-var (resolve source-fn)]
      (when source-var
        (let [new-var (intern *ns* target-name @source-var)]
          (alter-meta! new-var merge
                       (select-keys (meta source-var) [:arglists :doc])))))))
