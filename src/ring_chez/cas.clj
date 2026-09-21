(ns ring-chez.cas
  "One compare-and-set! whose false can be believed.

   Every once-only step in the adapter — claiming a connection, leaving the
   accept-pressure count, shutting a conn down, releasing its fd, reporting
   one failure per request, sending one websocket close — is a CAS whose
   false the caller reads as \"somebody else already did this\" and acts on by
   doing nothing. That reading needs the CAS to be STRONG: false only when the
   value is not the expected one.

   jolt's compare-and-set! below jolt-lang/jolt#1072 is not. It sits on Chez's
   $record-cas!, a single ldxr/stxr on AArch64, and the stxr fails whenever the
   core's exclusive monitor was cleared between the two — a context switch, or
   another core storing into the same reservation granule, which the acceptor
   writing :claim-by beside the worker's :owned? is. The primitive then answers
   false with the atom still holding the expected value, and the worker walks
   away from a live connection: one accept in ~1400 under ab on an M-series
   Mac, on both strategies, never on x86 (PR #39's open question, PR #40's
   `:unclaimed` and the pressure that leaked with it).

   cas! believes a false only once it has SEEN the value be something other
   than expected; while the value still is expected, the attempt is repeated.
   Identity comparison, as compare-and-set!'s own. On a strong primitive the
   loop never runs a second time; on the weak one it costs a deref on the rare
   failure. Drop this for compare-and-set! once :jolt/min-version carries the
   fixed primitive.")

(defn cas!
  "compare-and-set! that answers false only when the value has been seen not
  to be `expected`."
  [a expected v]
  (loop []
    (cond (compare-and-set! a expected v) true
          (identical? @a expected)        (recur)
          :else                           false)))
