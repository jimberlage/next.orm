(ns next.orm.core
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import (clojure.lang ISeq)
           (java.util UUID))
  (:refer-clojure :exclude [distinct take]))

(defn random-cursor []
  (str/replace (str (UUID/randomUUID)) "-" ""))

(defprotocol Queryable
  "Provides a queryable interface similar to what ActiveRecord would provide."
  (all [this])

  (find-by [this options])

  (take [this n])

  (distinct [this] [this value])

  (includes [this options])

  (where [this options])

  (order [this options]))

(s/def ::items
  (s/coll-of map?))

(s/def ::all? boolean?)

(s/def ::table-name
  (s/or :keyword keyword?
        :string string?))

(s/def ::model-definition
  (s/keys :opt-un [::table-name]))

(s/def ::model
  (s/or :keyword keyword?
        :string string?))

(s/def ::included-relations
  (s/or :map (s/map-of ::model ::included-relations)
        :list (s/coll-of ::included-relations)
        :model ::model))

(comment
 {:comment {:table-name :Comment
            :belongs-to [:post]}
  :post {:table-name :Post
         :primary-key :post_id
         :has-many {:comment {:foreign-key :commented_post_id}}}}

 {:comment {:belongs-to :post}
  :post {:has-many :comment}})

(s/def ::datasource any?)

(s/def ::schema
  (s/map-of keyword? ::model-definition))

(s/def ::cursor string?)

(s/def ::query-options
  (s/keys :req-un [::cursor
                   ::datasource
                   ::schema
                   ::model
                   ::items]
          :opt-un [::all?]))

(defn sql-quote [s]
  (format "\"%s\"" s))

(defn build-select-clause [schema models]
  (when (empty? models)
    (throw (ex-info "At least one model is required to use in the SELECT clause." {})))

  (->> models
       (map #(format "%s.*" (sql-quote (find-table-name schema %))))
       (str/join ", ")
       (format "SELECT %s")))

(defn build-join-for-included-relations [schema model included-relations]
  (cond
   (or (keyword? included-relations) (string? included-relations))
   ;; TODO: Look up table name for model and relation.
   (format "LEFT JOIN %s AS %s ON %s.%s = %s.%s" (sql-quote (find-table-name schema included-relations)))

   (map? included-relations)
   (->> included-relations
        (map #(build-join-for-included-relations schema (first %) (second %)))
        (str/join "\n"))

   (coll? included-relations)
   (->> included-relations
        (map #(build-join-for-included-relations schema model %))
        (str/join "\n"))

   :else
   (throw (ex-info "include requires a map, collection, keyword, or string argument" {}))))

(comment
 ;; included-relations looks like this:
 :site-trial-patients

 {:site-trial-patients []}

 ;; or this:
 {:site-trial-patients [:site-patient {:patient [:races]}]})

(defn build-from-clause-with-joins [schema model included-relations]
  (format
   "FROM %s AS %s\n%s"
   (sql-quote (find-table-name schema model))
   (build-join-for-included-relations schema model included-relations)))

(defn build-sql-statement [query-options]
  (format
   "DECLARE %s NO SCROLL CURSOR FOR \n%s\n%s\n%s"
   (:cursor query-options)
   (build-select-clause (:schema query-options))
   (build-from-clause-with-joins (:schema query-options) (:model query-options))
   (build-where-clause)))

;; TODO: ORDER BY needed?
(defn ^:private fetch-more* [query-options]
  ())

(defrecord Query [query-options]
  Queryable
  (find-by [this options])

  (take [this n])

  (distinct [this])

  (distinct [this value])

  (includes [this options])

  (where [this options])

  (order [this options])
  
  ISeq
  (first [this]
    (if-let [items (not-empty (:items this))]
      (first items)
      (let [])))
  
  (next [this])
  
  (more [this])
  
  (cons [this object]))

(defn query [datasource schema model]
  (->Query {:cursor (random-cursor)
            :datasource datasource
            :schema schema
            :model model
            :items []}))
