(ns snowpark-clj.core
  "The external API used by clients."
  (:require [potemkin :refer [import-vars]]
            [snowpark-clj.column]
            [snowpark-clj.dataframe]
            [snowpark-clj.functions]
            [snowpark-clj.schema]
            [snowpark-clj.session])
  (:refer-clojure :exclude [filter sort group-by take
                            and not or abs count max min]))

;; Firstly we re-export the functions from other namespaces that don't conflict with clojure core
;; with preserved docstrings and arglists

(import-vars
 [snowpark-clj.column
  gt lt eq geq leq neq]

 [snowpark-clj.session
  create-session
  close-session]

 [snowpark-clj.schema
  malli->schema]

 [snowpark-clj.dataframe
  create-dataframe table sql select where limit agg join
  collect row-count show save-as-table col schema]

 [snowpark-clj.functions
  lit avg sum upper lower])

;; Secondly we re-export the functions that do have conflicts with clojure core

;; Simple declare statements to help clj-kondo recognize the symbols
;; The actual implementations will be provided by potemkin import-vars
;; Note: some clj-kondo errors still won't report correctly for these specific functions
;; e.g. invalid-arity won't be reported in cases when it should be. This is a known issue
(declare filter sort group-by take and not or abs count max min)

;; clj-kondo doesn't understand potemkin's :rename syntax, so we suppress the redefined var warnings
#_{:clj-kondo/ignore [:redefined-var]}
(import-vars
 [snowpark-clj.column
  :refer [and-fn not-fn or-fn]
  :rename {and-fn and
           not-fn not
           or-fn or}]

 [snowpark-clj.dataframe
  :refer [df-filter df-sort df-group-by df-take]
  :rename {df-filter filter
           df-sort sort
           df-group-by group-by
           df-take take}]

 [snowpark-clj.functions
  :refer [abs-fn count-fn max-fn min-fn]
  :rename {abs-fn abs
           count-fn count
           max-fn max
           min-fn min}])
