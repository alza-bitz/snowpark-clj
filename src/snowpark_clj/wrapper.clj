(ns snowpark-clj.wrapper)

(defprotocol IWrappedSessionOptions
  "Protocol for accessing Snowpark objects that are wrapped with session options for convenience"
  (unwrap [this] "Get the Snowpark object from a wrapper")
  (unwrap-option [this option-key] "Get a session option from a wrapper")
  (unwrap-options [this] "Get all session options from a wrapper"))

(defn wrapper?
  "Check if the object is a Snowpark object wrapper"
  [obj]
  (satisfies? IWrappedSessionOptions obj))