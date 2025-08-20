(ns snowpark-clj.core
  "Main API for Snowpark Clojure wrapper"
  (:require [potemkin :refer [import-vars]] 
            [snowpark-clj.dataframe :as df]
            [snowpark-clj.functions :as fn]
            [snowpark-clj.schema]
            [snowpark-clj.session])
  (:refer-clojure :exclude [filter sort group-by take
                            count and or not abs max min]))

;; Re-export functions from other layers with preserved docstrings and arglists
(import-vars
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
   gt
   lt
   eq
   gte
   lte
   neq
   upper
   lower
   avg
   sum])

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
(let [alias-mappings [['fn/count-fn 'count]
                      ['fn/and-fn 'and]
                      ['fn/or-fn 'or]
                      ['fn/not-fn 'not]
                      ['fn/abs-fn 'abs]
                      ['fn/max-fn 'max]
                      ['fn/min-fn 'min]]]
  (doseq [[source-fn target-name] alias-mappings]
    (let [source-var (resolve source-fn)]
      (when source-var
        (intern *ns* target-name
                (with-meta @source-var
                  (merge (meta source-var)
                         {:arglists (:arglists (meta source-var))
                          :doc (:doc (meta source-var))})))))))
