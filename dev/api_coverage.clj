(ns api-coverage
  (:require
   [clojure.java.io :as io] 
   [net.cgrand.xforms.io :as xio]))

(def scan-result-join-key
  "Canonical join key for a scan result: [class method params-vector]" 
  (juxt :scanner/class :scanner/method :scanner/params))

(defn- build-index
  "Index results by join-key-fn."
  [join-key-fn results]
  (reduce (fn [index result]
            (let [k (join-key-fn result)]
              (update index k (fnil conj []) result)))
          {}
          results))

(defn- left-join
  "Returns a transducer that will left join all supplied results to right-results-index on join-fn."
  [join-key-fn right-results-index]
  (comp
   (map (fn [left-result]
          {:coverage/type :coverage/join-result
           :left-result left-result
           :right-results (get right-results-index (join-key-fn left-result) [])}))
   (mapcat (fn [join-result] (if (empty? (:right-results join-result))
                          [(assoc join-result :coverage/join-type :coverage/join-type-left)]
                          (map #(assoc join-result
                                       :right-result %
                                       :coverage/join-type :coverage/join-type-inner)
                               (:right-results join-result)))))
   (map #(assoc % :coverage/join-result (merge (:left-result %) (:right-result %))))
   (map #(dissoc % :right-results :left-result :right-result))))

(defn- join-stats
  "Returns a 3-arity reducing function that computes join stats for map values
   where :coverage/type is :coverage/join-result."
  [base-rf right-results]
  (fn
    ([] {:base-acc (base-rf) :left-count (volatile! 0) :inner-count (volatile! 0)})
    ([acc]
     (let [{:keys [base-acc left-count inner-count]} acc
           stats #:coverage{:type :coverage/join-stats
                            :left-count  @left-count
                            :inner-count @inner-count
                            :right-count (count right-results)}
           ;; write stats as another value using base-rf's step arity
           base-acc-after-stats (base-rf base-acc stats)]
       (base-rf base-acc-after-stats)))
    ([acc v]
     (let [{:keys [base-acc left-count inner-count]} acc
           _ (when (and (map? v)
                        (= :coverage/join-result (:coverage/type v)))
               (vswap! left-count inc)
               (when (= :coverage/join-type-inner (:coverage/join-type v))
                 (vswap! inner-count inc)))]
       (assoc acc :base-acc (base-rf base-acc v))))))

(comment
  (let [ns-scan-results (transduce identity conj (xio/edn-in "dev/scan-ns.edn"))
        ns-scan-results-index (build-index scan-result-join-key ns-scan-results)]
    (transduce
     (comp
      (left-join scan-result-join-key ns-scan-results-index)
      (filter #(= (:coverage/join-type %) :coverage/join-type-inner)))
     (join-stats conj ns-scan-results)
     ;;  conj
     (xio/edn-in (io/reader "dev/scan-package.edn")))))

(defn- initializing [f init]
  (fn
    ([] init)
    ([acc] (f acc))
    ([acc v] (f acc v))))

(defn generate
  "Generate API coverage and write out to an edn file."
  [{:keys [package-scan-results-in ns-scan-results-in out]
    :or {package-scan-results-in "dev/api-scanner-package.edn"
         ns-scan-results-in "dev/api-scanner-ns.edn"
         out "dev/api-coverage.edn"}}]
  (let [ns-scan-results (transduce identity conj (xio/edn-in ns-scan-results-in))
        ns-scan-results-index (build-index scan-result-join-key ns-scan-results)]
    (with-open [writer (io/writer out)]
      (transduce
       (left-join scan-result-join-key ns-scan-results-index)
       (join-stats (initializing xio/edn-out writer) ns-scan-results)
       (xio/edn-in package-scan-results-in))
      (println "Wrote coverage:" out))))

(comment
  (generate {}))