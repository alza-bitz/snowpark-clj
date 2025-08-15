(ns snowpark-clj.config
  "The internal API for the config schema and related functions."
  (:require
   [aero.core :as aero]
   [clojure.test.check.generators :as gen]
   [malli.core :as m]
   [malli.registry :as mr]
   [mask.aero]
   [mask.core :as mask]))

(def mask-schema
  [:fn {:error/message "must be a non-empty string tagged with #mask"
        :gen/gen (gen/fmap mask/mask (gen/not-empty gen/string-alphanumeric))}
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

(defn read-config
  "Load config from path using aero.
   
   Just wrapping aero/read-config to ensure that mask.aero is loaded and the #mask tag will be parsed correctly"
  [path]
  (aero/read-config path))

