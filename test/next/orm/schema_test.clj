(ns next.orm.schema-test
  (:require [clojure.test :refer [deftest is]]
            [next.orm.schema :as schema]))

(deftest belongs-to-example-1-works-as-expected
  (is (= {:model-name "comment" :relations {"post" {:foreign-key "parent_post_id" :type :belongs-to}}}
         (-> (schema/model :comment)
             (schema/belongs-to :post {:foreign-key :parent_post_id})))))

(deftest belongs-to-example-2-works-as-expected
  (is (= {:model-name "user" :relations {"manager" {:model-name "user" :type :belongs-to}}}
         (-> (schema/model :user)
             (schema/belongs-to :manager {:model-name :user})))))

(deftest get-joins-example-1-works-as-expected
  (is (= [{:from {:schema "public"}
           :to   {:schema "public"}}
          {:from {:schema "public"}
           :to   {:schema "public"}}
          {:from {:schema "public"}
           :to   {:schema "public"}}]
         (schema/get-joins
          (schema/schema
           (-> (schema/model :user)
               (schema/has-many :posts {:model-name :post}))
           (-> (schema/model :post)
               (schema/belongs-to :poster {:model-name :user})
               (schema/has-many :comments {:model-name :comment})
               (schema/has-many :commenters {:model-name :user :through :comments}))
           (-> (schema/model :comment)
               (schema/belongs-to :commenter {:model-name :user})
               (schema/belongs-to :post)))
          :post
          [:poster :commenters]))))
