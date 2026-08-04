(ns alicia.codec
  "TypeCodec adapters that return Clojure values.

  The driver's own codecs (resolved from a CodecRegistry) already handle the
  binary layout, nullability and CQL literal parsing/formatting of every CQL
  type, including tuples and UDTs. We simply wrap them so that decoded
  collection values come back as Clojure persistent collections instead of
  java.util ones.

  Encoding needs no conversion: Clojure persistent maps, sets and lists
  already implement the corresponding java.util interfaces that the driver's
  collection codecs consume."
  (:import [com.datastax.oss.driver.api.core.type DataType ListType MapType SetType]
           [com.datastax.oss.driver.api.core.type.codec TypeCodec]
           [java.util List Map Set]))

(defn adapt
  "Wraps delegate so that decoded/parsed values are converted with to-clj and
  values about to be encoded/formatted are converted back with to-java."
  [^TypeCodec delegate to-clj to-java]
  (reify TypeCodec
    (getJavaType [_] (.getJavaType delegate))
    (getCqlType [_] (.getCqlType delegate))
    (^boolean accepts [_ ^Object value]
      (boolean (and (some? value) (.accepts delegate (to-java value)))))
    (encode [_ value protocolVersion]
      (when (some? value)
        (.encode delegate (to-java value) protocolVersion)))
    (decode [_ bytes protocolVersion]
      (some-> (.decode delegate bytes protocolVersion) to-clj))
    (format [_ value]
      (when (some? value)
        (.format delegate (to-java value))))
    (parse [_ value]
      (some-> (.parse delegate value) to-clj))))

(defn- ->clj
  "Deeply converts decoded java.util collections to Clojure persistent ones.
  Other decoded values (scalars, ByteBuffers, TupleValue, UdtValue, ...) pass
  through unchanged."
  [v]
  (cond
    (instance? Map v)  (into {} (map (fn [[k x]] [(->clj k) (->clj x)])) v)
    (instance? Set v)  (into #{} (map ->clj) v)
    (instance? List v) (into [] (map ->clj) v)
    :else v))

(defn clojure-codec
  "Returns the codec to use for cql-type: delegate itself for scalar types, or
  an adapted delegate that decodes CQL collections (at any nesting depth) into
  Clojure persistent collections (map/set/vector) for map/set/list types."
  [^DataType cql-type ^TypeCodec delegate]
  (if (or (instance? MapType cql-type)
          (instance? SetType cql-type)
          (instance? ListType cql-type))
    (adapt delegate ->clj identity)
    delegate))
