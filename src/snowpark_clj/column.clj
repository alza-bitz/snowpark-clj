(ns snowpark-clj.column)

(defn parse-quoted
  "Parses the column name according to whether it is quoted or not."
  [col-name]
  (when col-name 
    (->> col-name
       (re-matches #"\"(.*)\"|(.*)")
       rest
       (zipmap [:quoted :unquoted])
       (merge {:col-name col-name
               :unquoted ""}))))

(defn quoted
  "Transforms the column name to avoid issues with parentheses in keywords.
   
   Aggregated column names are themselves quoted, for example '\"COUNT(<COLNAME>)\"', '\"AVG(<COLNAME>)\"', etc.
   These will be unquoted and transformed to 'COUNT-<COLNAME>', 'AVG-<COLNAME>'.
   
   Any other quoted column names are not supported and an exception will be thrown."
  [col-name]
  (when-let [{:keys [quoted unquoted]} (parse-quoted col-name)]
    (or
     unquoted
     (if-let [matches (re-matches #"(\w+)\((.+)\)" quoted)]
       (str (nth matches 1) "-" (nth matches 2))
       (throw (ex-info (str "Quoted column names are not supported: " col-name) {:col-name col-name}))))))