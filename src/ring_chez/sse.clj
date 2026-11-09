(ns ring-chez.sse
  (:require [clojure.core.async :as async]
            [clojure.string :as str]))

(defn format-event
  "Event map {:id :event :data :retry} -> wire string. Multi-line data is
  split (CRLF / LF / bare CR) into one data: line per source line, per the
  SSE spec; the trailing blank line dispatches the event."
  [{:keys [id event data retry]}]
  (let [sb (StringBuilder.)]
    (when id (.append sb (str "id: " id "\r\n")))
    (when event (.append sb (str "event: " event "\r\n")))
    (when retry (.append sb (str "retry: " retry "\r\n")))
    (doseq [line (str/split (str data) #"\r\n|\n|\r")]
      (.append sb (str "data: " line "\r\n")))
    (.append sb "\r\n")
    (.toString sb)))

(defn event-response
  "Ring response map streaming Server-Sent Events from ch, a channel the
  handler produces on. Put event maps on ch ({:id :event :data :retry}, as
  format-event takes) or strings already formatted, e.g. by send!; end the
  stream by closing ch.

  The adapter only frames strings and byte arrays, so the body is a second
  channel carrying ch's values as formatted events. When the client goes away
  the adapter closes that channel; the next value taken from ch finds it closed
  and ch is closed in turn, so the producer's following put returns false."
  [ch]
  (let [out (async/chan)]
    (async/go
      (try
        (loop []
          (let [v (async/<! ch)]
            (if (nil? v)
              (async/close! out)
              (if (async/>! out (if (map? v) (format-event v) v))
                (recur)
                (async/close! ch)))))
        (catch Throwable _
          (async/close! out)
          (async/close! ch))))
    {:status 200
     :headers {"Content-Type" "text/event-stream"
               "Cache-Control" "no-cache"}
     :body out}))

(defn send!
  "Put one event map on the SSE channel; returns the put result (false once
  the channel is closed, e.g. the client went away)."
  [ch event]
  (async/>!! ch (format-event event)))
