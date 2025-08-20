(ns snowpark-clj.schemas 
  (:require
   [malli.core :as m]))

;; Malli schema based on test_data.csv structure (Feature 4)
(def employee-schema
  (m/schema [:map
             [:id :int]
             [:name :string]
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]]))

(def employee-schema-with-string-keys
  (m/schema [:map
             ["ID" :int]
             ["NAME" :string]
             ["DEPARTMENT" [:enum "Engineering" "Marketing" "Sales"]]
             ["SALARY" [:int {:min 50000 :max 100000}]]]))

(def employee-schema-with-optional-keys
  (m/schema [:map
             [:id :int]
             [:name :string] 
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]
             [:age {:optional true} :int]]))

(def employee-schema-with-nil-values
  (m/schema [:map
             [:id :int]
             [:name :string] 
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]
             [:age :nil]]))

(def employee-schema-with-maybe-nil-values
  (m/schema [:map
             [:id :int]
             [:name :string]
             [:department [:enum "Engineering" "Marketing" "Sales"]]
             [:salary [:int {:min 50000 :max 100000}]]
             [:age [:or int? nil?]]]))
