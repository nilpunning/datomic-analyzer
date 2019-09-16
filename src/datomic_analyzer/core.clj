(ns datomic-analyzer.core
  (:require [datomic.api :as d]))

(defn attribute-stats
  "Returns a seq of the following format:
  [attribute [total-count add-count retract-count]]
  sorted by total-count."
  [conn]
  (sort-by
   (fn [[_ [total _ _]]] total)
   (reduce
    (fn [acc tx]
      (reduce
       (fn [acc [_ a _ _ added?]]
         ; TODO add some logging that indicates percentage done
         (update
          acc
          (:db/ident (d/pull (d/db conn) [:db/ident] a))
          (fn [[total adds retracts]]
            (let [total    (or total 0)
                  adds     (or adds 0)
                  retracts (or retracts 0)]
              (if added?
                [(inc total) (inc adds) retracts]
                [(inc total) adds (inc retracts)])))))
       acc
       (:data tx)))
    {}
    (d/tx-range (d/log conn) nil nil))))

(comment
 (attribute-stats (d/connect user/uri))
 )
