(ns snowpark-clj.core
  "Main API for Snowpark Clojure wrapper"
  (:require [potemkin :refer [import-vars]]
            [snowpark-clj.convert]
            [snowpark-clj.dataframe :as df]
            [snowpark-clj.functions]
            [snowpark-clj.session])
  (:refer-clojure :exclude [filter sort group-by count take]))

;; Re-export functions from other layers with preserved docstrings and arglists
(import-vars
  [snowpark-clj.session
   create-session
   close-session
   with-session]
  
  [snowpark-clj.dataframe
   create-dataframe
   table
   sql
   select
   where
   limit
   join
   collect
   show
   first-row
   save-as-table
   schema]
  
  [snowpark-clj.functions
  ;;  col
   lit
   gt
   lt
   eq
   gte
   lte
   not-equal
   and-fn
   or-fn
   not-fn
   abs-fn
   max-fn
   min-fn
   upper
   lower]
  
  [snowpark-clj.convert
   malli-schema->snowpark-schema])

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

(def ^{:arglists (:arglists (meta #'df/df-count))
       :doc (:doc (meta #'df/df-count))}
  count df/df-count)

(def ^{:arglists (:arglists (meta #'df/df-take))
       :doc (:doc (meta #'df/df-take))}
  take df/df-take)
