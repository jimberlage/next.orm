(ns next.orm.schema
  (:require [clojure.spec.alpha :as s]))

(s/def ::foreign-key string?)

(s/def ::model-name string?)

(s/def ::through string?)

(s/def ::belongs-to-relation
  (s/and
   (s/keys :opt-un [::foreign-key
                    ::model-name])
   #(= (:type %) :belongs-to)))

(s/def ::has-many-relation
  (s/and
   (s/keys :opt-un [::foreign-key
                    ::model-name
                    ::through])
   #(= (:type %) :has-many)))

(s/def ::has-one-relation
  (s/and
   (s/keys :opt-un [::foreign-key
                    ::model-name])
   #(= (:type %) :has-one)))

(s/def ::relation
  (s/or :belongs-to ::belongs-to-relation
        :has-many ::has-many-relation
        :has-one ::has-one-relation))

(s/def ::relation-name string?)

(s/def ::relations
  (s/map-of ::relation-name ::relation))

(s/def ::database-schema string?)

(s/def ::primary-key string?)

(s/def ::table-name string?)

(s/def ::model
  (s/keys :opt-un [::database-schema
                   ::model-name
                   ::primary-key
                   ::relations
                   ::table-name]))

(s/def ::schema
  (s/map-of ::model-name ::model))

(defn ^:private stringify-keywords [m ks]
  (reduce
   (fn [m' k]
     (if (and (contains? m' k) (keyword? (get m' k)))
       (update m' k name)
       m'))
   m
   ks))

(defn ^:private stringify-model-and-column-names [m]
  (stringify-keywords m [:database-schema :model-name :table-name :through :primary-key :foreign-key]))

(defn model
  "Begins a model definition for an object in the DB.

  **Examples:**

  ```clojure
  ;; TODO: Example
  ```"
  [model-name]
  {:model-name (name model-name)})

(defn table-name
  "Overrides a table name for the model.  If your table name doesn't follow the convention next.orm expects, this lets you specify the right table name.

  **Examples:**

  ```clojure
  ;; TODO: Example
  ```"
  [model table-name]
  (assoc model :table-name (name table-name)))

(defn primary-key [model column]
  (assoc model :primary-key (name column)))

(defn belongs-to
  "Creates a belongs-to relationship between two models.

  **Examples:**

  Forum posts with an owner, plus a non-conforming foreign key syntax

  ```clojure
  (-> (model :comment)
      (belongs-to :post {:foreign-key :parent_post_id})
  ;;_=> {:model-name \"comment\" :relations {\"post\" {:foreign-key \"parent_post_id\" :type :belongs-to}}}
  ```

  Self joins, employees with a manager

  ```clojure
  (-> (model :user)
      (belongs-to :manager {:model-name :user})
  ;;_=> {:model-name \"user\" :relations {\"manager\" {:model-name \"user\" :type :belongs-to}}}
  ```"
  ([model relation-name]
   (belongs-to model relation-name {}))
  ([model relation-name relation-options]
   (let [relation-options' (-> relation-options
                               (assoc :type :belongs-to)
                               stringify-model-and-column-names)]
     (assoc-in model [:relations (name relation-name)] relation-options'))))

(defn has-one
  ""
  ([model relation-name]
   (has-one model relation-name {}))
  ([model relation-name relation-options]
   (let [relation-options' (-> relation-options
                               (assoc :type :has-one)
                               stringify-model-and-column-names)]
     (assoc-in model [:relations (name relation-name)] relation-options'))))

(defn has-many
  ""
  ([model relation-name]
   (has-many model relation-name {}))
  ([model relation-name relation-options]
   (let [relation-options' (-> relation-options
                               (assoc :type :has-one)
                               stringify-model-and-column-names)]
     (assoc-in model [:relations (name relation-name)] relation-options'))))

(defn assoc-model [schema & models]
  (reduce
   (fn [schema model]
     ;; TODO: Error if there is no name.
     (assoc schema (:model-name model) (dissoc model :model-name)))
   schema
   models))

(defn schema [& models]
  (apply assoc-model {} models))

(comment
  (schema
   (-> (model :post)
       (table-name :Post)
       (primary-key :post_id)
       (belongs-to :owner {:model-name :user :foreign-key :post_owner_id})
       (has-many :comments)
       (has-many :commenters {:through :comments}))
   (model :user)
   (-> (model :comment)
       (table-name :Comment)
       (belongs-to :poster {:model-name :user}))))

(defn missing-model-exception [schema model-name]
  (ex-info
   (format "Your schema is expected to have an entry for each model.  We couldn't find the model %s in your schema.  This may mean that the schema is nil when you didn't intend it to be, or that your model is a table name instead of one of the keys of your schema, or something else." (pr-str model-name))
   {:type ::missing-model-exception
    :schema schema
    :model-name model-name}))

(defn get-model [schema model-name]
  (or (get schema model-name)
      (throw (missing-model-exception schema model-name))))

(defn get-database-schema [schema model-name]
  (-> (get-model schema model-name)
      (get :database-schema "public")))

(defn get-table-name [schema model-name]
  (-> (get-model schema model-name)
      (get :table-name model-name)))

(defn get-primary-key [schema model-name]
  (let [database-schema (get-database-schema schema model-name)
        table-name (get-table-name schema model-name)
        primary-key (-> (get-model schema model-name)
                        (get :primary-key "id"))]
    {:schema database-schema
     :table table-name
     :column primary-key
     :table-alias (str database-schema "_" model-name)}))

(defn missing-relation-exception [schema model-name relation-name]
  (ex-info
   (format "Your schema may be missing a relation.  We couldn't find the relationship %s for model %s in your schema.  This may mean that the schema is nil when you didn't intend it to be, or that your model is a table name instead of one of the keys of your schema, or something else." (pr-str relation-name) (pr-str model-name))
   {:type ::missing-relation-exception
    :schema schema
    :model model-name
    :relation relation-name}))

(defn get-relation [schema model-name relation-name]
  (or (-> (get-model schema model-name)
          (get-in [:relations relation-name]))
      (throw (missing-relation-exception schema model-name relation-name))))

(defn belongs-to? [relation]
  (= (:type relation) :belongs-to))

(defn has-many? [relation]
  (= (:type relation) :has-many))

(defn has-many-through? [relation]
  (and (has-many? relation) (contains? relation :through)))

(defn has-one? [relation]
  (= (:type relation) :has-one))

(defn get-direct-relation-join [schema model-name relation-name]
  (let [database-schema (get-database-schema schema model-name)
        relation (get-relation schema model-name relation-name)
        foreign-key (get relation :foreign-key (str relation-name "_id"))
        relation-model-name (get relation :model-name relation-name)]
    (case (:type relation)
      :belongs-to
      ()
      
      :has-many
      ()
      
      :has-one
      ())
    ;; TODO: Need to change to/from when a belongs-to
    {:from {:schema database-schema
            :table (get-table-name schema model-name)
            :column foreign-key
            :alias (str database-schema "_" model-name "_" relation-name)}
     :to (get-primary-key schema relation-model-name)}))

(defn get-all-relation-joins [schema model-name relation-name]
  (let [relation (get-relation schema model-name relation-name)]
    (if (has-many-through? relation)
      (let [through-relation-name (:through relation)
            through-relation (get-relation schema model-name through-relation-name)
            through-relation-model-name (get through-relation :model-name through-relation-name)]
        [(get-direct-relation-join schema model-name through-relation-name) (get-direct-relation-join schema through-relation-model-name relation-name)])
      [(get-direct-relation-join schema model-name relation-name)])))

(defn get-joins
  ""
  [schema model-name relation-names]
  (->> relation-names
       (reduce
        (fn [{:keys [seen ordered-joins]} relation-name]
          (let [relation-joins (get-all-relation-joins schema model-name relation-name)
                ordered-joins' (->> relation-joins
                                    (remove #(contains? seen %))
                                    (concat ordered-joins))
                seen' (into seen relation-joins)]
            {:seen seen' :ordered-joins ordered-joins'}))
        {:seen #{} :ordered-joins []})
       :ordered-joins))
