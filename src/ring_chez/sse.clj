(ns ring-chez.sse
  (:require [clojure.core.async :as async]
            [clojure.string :as str]))

(defn event-response
  "Ring response map streaming Server-Sent Events from ch, a channel the
  handler produces on. The adapter streams channel bodies chunked; end the
  stream by closing the channel."
  [ch]
  {:status 200
   :headers {"Content-Type" "text/event-stream"
             "Cache-Control" "no-cache"}
   :body ch})

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

(defn send!
  "Put one event map on the SSE channel; returns the put result (false once
  the channel is closed, e.g. the client went away)."
  [ch event]
  (async/>!! ch (format-event event)))
