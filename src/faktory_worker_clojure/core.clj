(ns faktory-worker-clojure.core
  (:require
    [clojure.tools.logging :as log]
    [clojure.string :as string]
    [schema.core :as schema]
    [aleph.tcp :as tcp]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [gloss.core :as gloss]
    [gloss.io :as io]
    [cheshire.core :as json]
    [clojure.walk :refer [keywordize-keys]]
    [faktory-worker-clojure.pool :as pool]))

(def manifold-timeout 5000)
(def heartbeat-interval 15000)
(def host "127.0.0.1")
(def port 7419)
(def scheduler-pool-size 3)

(def not-empty-str (schema/constrained schema/Str not-empty))
(def at-least-60-secs #(>= % 60))
(def nine-is-high-one-is-low #(and (>= % 1)
                                   (<= % 9)))
(def minus-one-goes-to-dead-zero-fails-silently-above-retry #(>= % -1))

(def job-schema
  {:queue not-empty-str
   :reserve_for (schema/constrained schema/Int
                                    at-least-60-secs)
   :priority (schema/constrained schema/Int
                                 nine-is-high-one-is-low)
   :retry (schema/constrained schema/Int
                              minus-one-goes-to-dead-zero-fails-silently-above-retry)
   (schema/optional-key :at) not-empty-str})

(def params-schema
  {:job-defaults job-schema
   :processors [{:threads (schema/constrained schema/Int
                                              pos?)
                 :queues [not-empty-str]}]
   (schema/optional-key :error-handler) (schema/=> schema/Any schema/Any)})

(def base-defaults
  {:job-defaults {:queue "default"
                  :reserve_for 1800
                  :priority 5
                  :retry 3}
   :processors [{:threads 10
                 :queues ["default"]}]})

(defn- put
  ([{:keys [socket]} msg]
   (log/info :put----------------------> msg)
   @(s/put! socket msg))
  ([client msg json-map]
   (put client (str msg " " (json/generate-string json-map)))))

(defn- try-take [{:keys [socket]}]
  @(s/try-take! socket :empty manifold-timeout :timeout))

(declare result)

(defn- maybe-error [client message]
  (when (re-find #"^-" message)
    (log/error (Exception. :faktory-error) message)
    :err))

(def supported-faktory-version 2)

(defn- faktory-version-matches? [version]
  (= version supported-faktory-version))

(defn- maybe-handshake [{:keys [wid] :as client} message]
  (when-let [[_ params-string] (re-find #"^\+HI (.+)" message)]
    (let [{:keys [v s i]} (keywordize-keys (json/parse-string params-string))
          options (merge {:v supported-faktory-version}
                         (when wid
                           {:wid wid
                            :labels ["clojure"]})
                         ;; TODO: passwords
                         #_(when s
                           {:pwdhash (pwdhash s i)}))]
      (assert (faktory-version-matches? v)))
    (if wid
      (put client "HELLO" {:wid wid
                           :labels ["clojure"]
                           :v supported-faktory-version})
      (put client "HELLO" {:v supported-faktory-version}))
    (result client)))

(defn- maybe-ok [client message]
  (when (re-find #"^\+OK$" message)
    :ok))

(defn- maybe-set-length [client message]
  (when-let [[_ msg] (re-find #"^\$(.+)" message)]
    (let [length (Integer/parseInt msg)]
      (if (= length -1)
        :nop
        (result client)))))

(defn- job-or-heartbeat-info-json [client message]
  (json/parse-string message))

(defn- result [client]
  (let [message (try-take client)]
    (or
      (maybe-error client message)
      (maybe-handshake client message)
      (maybe-ok client message)
      (maybe-set-length client message)
      (job-or-heartbeat-info-json client message))))

(def protocol
  (gloss/compile-frame
    (gloss/string :utf-8 :delimiters ["\r\n"])))

(defn- wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode protocol %) out)
      s)
    (s/splice
      out
      (io/decode-stream s protocol))))

(defn- prepared-client
  [{:keys [host port]} & [worker-params]]
  (try
    (let [socket @(d/chain (tcp/client {:host host :port port})
                           #(wrap-duplex-stream protocol %))
          client (merge worker-params
                        {:socket socket})]
      (assert (= (result client) :ok))
      client)
    (catch Exception e
      (log/error e :failed-to-connect))))

(defn- start-working
  [{:keys [wid fetch? worker-atom] :as client} queues]
  (future
    (while @fetch?
      (put client (str "FETCH " (string/join " " queues)))
      (let [job (result client)]
        (when-not (= job :nop)
          (try
            (let [{:keys [registered-jobs]} @worker-atom]
              (apply
                (get registered-jobs
                     (get job "jobtype"))
                (get job "args")))
            (put client "ACK" {:jid (get job "jid")})
            (assert (= (result client) :ok))
            (catch Exception e
              (log/error e {:wid wid :job job})
              (when-let [error-handler (:error-handler @worker-atom)]
                (error-handler e job))
              (put client "FAIL" {:jid (get job "jid")
                                  :errtype (str (.getClass e))
                                  :message (.getMessage e)
                                  :backtrace (mapv str (.getStackTrace e))})
              (assert (= (result client) :ok)))))))))

(defn- random-id [] (str (java.util.UUID/randomUUID)))

(defn- schedule-job-for-scheduler
  [client fun-name fun-args settings]
  (schema/validate job-schema settings)
  (put client "PUSH" (merge {:jid (random-id)
                             :jobtype fun-name
                             :args fun-args}
                            settings))
  (assert (= (result client) :ok)))

(defn schedule-job [worker-atom fun-name fun-args & [optional-settings]]
  (let [{:keys [job-defaults scheduler-pool]} @worker-atom
        settings (merge job-defaults optional-settings)]
    (pool/with-obj
      scheduler-pool
      #(schedule-job-for-scheduler % fun-name fun-args settings))))

(defn register-job [worker-atom fun-name fun]
  (swap! worker-atom
         assoc-in
         [:registered-jobs fun-name]
         fun))

(defn- add-scheduler-pool [worker]
  (assoc
    worker
    :scheduler-pool
    (pool/init (repeatedly scheduler-pool-size #(prepared-client worker)))))

(defn new-processor-client [worker-atom queues]
  (let [{:keys [host port]} @worker-atom
        params {:wid (random-id)
                :queues queues
                :beat? (volatile! true)
                :fetch? (volatile! true)
                :worker-atom worker-atom}
        processor-client (prepared-client host port params)]
    (start-working processor-client queues)
    processor-client))

(defn- spawn-processors [worker-atom]
  (swap!
    worker-atom
    update
    :processors
    (fn [processors]
      (reduce
        (fn [a {:keys [threads queues]}]
          (concat a (repeatedly threads #(new-processor-client worker-atom queues))))
        []
        processors)))
  worker-atom)

(defn- beat-for-processor [beat-client {:keys [wid beat? fetch?] :as processor}]
  (put beat-client "BEAT" {:wid wid})
  (let [heartbeat (result beat-client)]
    (when-not (= heartbeat :ok)
      (condp = (get heartbeat "state")
        "quiet" (do (log/info :quieted wid)
                    (vreset! fetch? false))
        "terminate" (do (log/info :terminated wid)
                        (vreset! fetch? false)
                        (vreset! beat? false))
        #_else (log/error (Exception. :bad-heartbeat-from-faktory))))))

(defn- beat [worker-atom]
  (swap! worker-atom assoc :alive? true)
  (future
    (let [beat-client (prepared-client @worker-atom)]
      (while (:alive? @worker-atom)
        (Thread/sleep heartbeat-interval)
        (swap! worker-atom
               update
               :processors
               #(->> %
                     (filter :beat?)
                     (map (partial beat-for-processor beat-client)))))))
  worker-atom)

(defn- valid-params [{:keys [job-defaults processors scheduler]}]
  (let [params (-> base-defaults
                   (update :job-defaults #(merge % job-defaults))
                   (update :processors #(or processors %)))]
    (schema/validate params-schema params)
    params))

(defn- add-host-port [params]
  (assoc params :host host :port port))

(defn init [input-params]
  (-> input-params
      valid-params
      add-host-port
      add-scheduler-pool
      atom
      spawn-processors
      beat))


(comment
  (def worker (init {:job-defaults {}
                     :processors [{:threads 1
                                   :queues ["default"]}]}))

  (register-job worker :asdf (fn [a b] (log/info :asdf (+ a b))))
  (schedule-job worker :asdf [1 2])
  (schedule-job worker :dupa [2 "asdf"] {:retry 1})

  )

;; test error handler
