(ns kinsky-example.core
  (:require [kinsky.client :as client]
            [kinsky.async :as async]
            [clojure.spec.alpha :as s]))

(defn poll [c]
  (try
    (client/poll! c 1000)
    (catch Exception e
      (println (.getMessage e)))))

(defn -main [& args]
  (let [topic "test"
        c (client/consumer {:bootstrap.servers "localhost:9092"
                            :group.id "mygroup"}
                           (client/string-deserializer)
                           (client/json-deserializer))]
    (client/subscribe! c topic)
    (loop []
      (let [{:keys [count by-topic] :as message} (poll c)]
        (when (and (not (nil? count)) (pos? count))
          (doseq [message (get (:by-topic message) topic)]
            (println message)))
        (recur)))))
