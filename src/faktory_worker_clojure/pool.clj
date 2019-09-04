(ns faktory-worker-clojure.pool
  (:require
    [manifold.stream :as s]
    [clojure.tools.logging :as log]))

(defn init [init-values]
  (let [pool-stream (s/stream)]
    (doseq [value init-values]
      (s/put! pool-stream value))
    pool-stream))

(defn with-obj [pool-stream f]
  (let [e @(s/take! pool-stream)]
    (try
      (f e)
      (finally
        (s/put! pool-stream e)))))
