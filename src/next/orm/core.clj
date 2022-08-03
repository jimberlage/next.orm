(ns next.orm.core
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import (clojure.lang ISeq))
  (:refer-clojure :exclude [distinct take]))

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

(s/def ::model keyword?)

(s/def ::datasource any?)

(s/def ::table-name keyword?)

(s/def ::model-definition
  (s/keys :opt-un [::table-name]))

(s/def ::schema
  (s/map-of keyword? ::model-definition))

(s/def ::query-options
  (s/keys :req-un [::datasource
                   ::schema
                   ::model
                   ::items]
          :opt-un [::all?]))

(defn find-table-name [schema model]
  (let [model-definition (get schema model)
        custom-table-name (:table-name model-definition)]
    (name (or custom-table-name model))))

(defn build-select-clause [schema models order-by]
  (when (empty? models)
    (throw (ex-info "At least one model is required to use in the SELECT clause." {})))

  (let [table-terms (->> models
                         (map (fn [model]
                                (format "\"%s\".*" (find-table-name schema model))))
                         (str/join ", "))
        order-by-term (or order-by "")]
    (format "SELECT %s, ROW_NUMBER() OVER (%s) AS rn" table-terms order-by-term)))

(defn build-graph [schema model included-relations]
  ())

(defn build-from-clause-with-joins [schema models]
  ())

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
  (->Query {:datasource datasource :schema schema :model model :items []}))
