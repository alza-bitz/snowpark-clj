(ns api-scanner
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str] 
   [net.cgrand.xforms.io :as xio])
  (:import
   [io.github.classgraph ClassGraph]))

(defn- class->javadoc-url
  "Convert a class name to its javadoc URL."
  [javadoc-base class-name]
  (let [path (-> class-name
                 (str/replace "." "/")
                 (str ".html"))]
    (str javadoc-base "/" path)))

(defn- cg-scan [package]
  (-> (ClassGraph.)
      (.enableAllInfo)
      (.acceptPackagesNonRecursive (into-array String [package]))
      (.scan)))

(defn- class-info->scan-result
  "Returns a transducer that converts ClassGraph ClassInfo entries into scan results."
  [javadoc-base]
  (comp (mapcat #(.getDeclaredMethodInfo %))
        (filter (fn [mi] (and (.isPublic mi) (not (.isAbstract mi)))))
        (map (fn [mi]
               (let [ci (.getClassInfo mi)]
                 #:scanner{:class  (.getName ci)
                           :method (.getName mi)
                           :params (mapv #(str (.getTypeDescriptor %)) (.getParameterInfo mi))
                           :return (str (.getTypeDescriptor mi))
                           :javadoc (class->javadoc-url javadoc-base (.getName ci))})))))

(comment
  (with-open [cg-scan-result (cg-scan "com.snowflake.snowpark_java")]
    (transduce
     (class-info->scan-result "https://javadoc.snowflake.com/snowpark-java/1.16.0")
     conj
     (take 1 (.getAllClasses cg-scan-result)))))

(defn scan-package
  "Scan the given Java package and write the results to an edn file."
  [{:keys [package javadoc-base out]
    :or {out "dev/api-scanner-package.edn"}}]
  {:pre [(string? package)
         (string? javadoc-base)
         (string? out)]}
  (println "Scanning package:" package)
  (with-open [cg-scan-result (cg-scan package)
              writer (io/writer out)]
    (transduce
     (class-info->scan-result javadoc-base)
     xio/edn-out
     writer
     (.getAllClasses cg-scan-result))
    (println "Wrote scan results:" out)))

(comment
  (scan-package {:package "com.snowflake.snowpark_java"
                 :javadoc-base "https://javadoc.snowflake.com/snowpark-java/1.16.0"}))

(defn- file->github-url
  "Convert to a GitHub source URL."
  [github-base file]
  (let [base (str/replace github-base #"/$" "")]
    (str base "/" file)))

(defn- ns-publics->scan-result
  "Returns a transducer that converts public namespace mappings to scan results."
  [github-base]
  (let [required-keys #{:scanner/class :scanner/method :scanner/params}]
    (comp (map (fn [[_ var]] (meta var)))
          ;; (filter (fn [metadata] (every? #(contains? metadata %) required-keys)))
          (map #(merge {:scanner/ns (str (:ns %))
                        :scanner/name (str (:name %))
                        :scanner/github (file->github-url github-base (:file %))}
                       (select-keys % required-keys)))
          ;; flatten params into a scan result for each if there are nested params
          (mapcat (fn [scan-result]
                    (if (some vector? (:scanner/params scan-result))
                      (map #(assoc scan-result :scanner/params %) (:scanner/params scan-result))
                      [scan-result]))))))

(comment
  (require 'snowpark-clj.core :reload-all))

(comment
  (transduce (ns-publics->scan-result "https://github.com/alza-bitz/snowpark-clj/tree/main/src") conj (ns-publics 'snowpark-clj.core)))

(defn scan-ns
  "Scan the given Clojure namespace for public vars that have `:scanner/*` metadata
   and write the results out to an edn file."
  [{:keys [namespace github-base out]
    :or {out "dev/api-scanner-ns.edn"}}]
  {:pre [(symbol? namespace)
         (string? github-base)
         (string? out)
         (nil? (require namespace :reload-all))]}
  (println "Scanning namespace:" namespace)
  (with-open [writer (io/writer out)]
    (transduce (ns-publics->scan-result github-base) xio/edn-out writer (ns-publics namespace))
    (println "Wrote scan results: " out)))

(comment
  (scan-ns {:namespace 'snowpark-clj.core}))