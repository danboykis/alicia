(ns alicia.codec
  (:require [clojure.edn :as edn])
  (:import [com.datastax.oss.driver.api.core.type DataType DataTypes]
           [com.datastax.oss.driver.api.core.type.codec TypeCodec]
           [com.datastax.oss.driver.api.core.type.reflect GenericType]
           [java.nio ByteBuffer]
           [java.util List Map Set]))

(defn- decode-element [^ByteBuffer input ^TypeCodec tc protoVersion]
  (let [elem-size (.getInt input)
        e (if (< elem-size 0)
            nil
            (let [encoded-e (doto (.slice input) (.limit elem-size))
                  e (.decode tc encoded-e protoVersion)]
              (.position input (+ elem-size (.position input)))
              e))]
    e))

(defn- clj-map-codec [^DataType cqlType ^TypeCodec keyCodec ^TypeCodec valueCodec]
  (let [java-type (GenericType/mapOf (.getJavaType keyCodec) (.getJavaType valueCodec))]
    (reify TypeCodec
      (getJavaType [_] java-type)
      (getCqlType [_] cqlType)
      (^boolean accepts [_ ^Object value]
        (if (instance? Map value)
          (if (empty? value)
            true
            (let [[k v] (first value)]
              (boolean (and (.accepts ^TypeCodec keyCodec k) (.accepts ^TypeCodec valueCodec v)))))
          false))
      (encode [_ val protoVersion]
        (when (some? val)
          (let [encoded-elems (make-array ByteBuffer (* 2 (count val)))
                to-alloc (loop [[k v] val
                                counter 0
                                members 4]
                           (if (and (some? k) (some? v))
                             (let [encoded-key (.encode keyCodec k protoVersion)
                                   encoded-val (.encode valueCodec v protoVersion)]
                               (aset encoded-elems counter encoded-key)
                               (aset encoded-elems (inc counter) encoded-val)
                               (recur (next val)
                                      (inc (inc counter))
                                      (+  members
                                          4
                                          (.remaining encoded-key)
                                          4
                                          (.remaining encoded-val))))
                             members))
                ^ByteBuffer result (doto (ByteBuffer/allocate to-alloc) (.putInt (count val)))]

            (doseq [encoded-elem encoded-elems]
              (.putInt result (.remaining encoded-elem))
              (.put result ^ByteBuffer encoded-elem))
            (.flip result)
            result)))
      (decode [_ bytes protoVersion]
              (if (or (nil? bytes) (zero? (.remaining ^ByteBuffer bytes)))
                {}
                (let [input (.duplicate bytes)
                      size  (.getInt input)]
                  (loop [counter 0
                         result (transient {})]
                    (if (= counter size)
                      (persistent! result)
                      (let [k (decode-element input keyCodec protoVersion)
                            v (decode-element input valueCodec protoVersion)]
                        (recur (inc counter) (assoc! result k v))))))))
      (format [_ value] (str value))
      (parse [_ value]  (edn/read-string value)))))

(defn- encode-helper [val protoVersion ^TypeCodec elemCodec]
  (when (some? val)
    (let [encoded-elems (make-array ByteBuffer (count val))
          to-alloc (loop [[e & es] val
                          counter 0
                          members 4]
                     (if (some? e)
                       (let [encoded-e (.encode elemCodec e protoVersion)]
                         (aset encoded-elems counter encoded-e)
                         (recur es
                                (inc (inc counter))
                                (+  members
                                    4
                                    (.remaining encoded-e))))
                       members))
          result (doto (ByteBuffer/allocate to-alloc) (.putInt (count val)))]

      (doseq [encoded-elem encoded-elems]
        (.putInt result (.remaining encoded-elem))
        (.put result ^ByteBuffer encoded-elem))
      (.flip result)
      result)))

(defn- decode-helper [bytes protoVersion ^TypeCodec elemCodec empty-coll]
  (if (or (nil? bytes) (zero? (.remaining ^ByteBuffer bytes)))
    empty-coll
    (let [input (.duplicate bytes)
          num-elems  (.getInt input)]
      (loop [counter 0
             result (transient empty-coll)]
        (if (= counter num-elems)
          (persistent! result)
          (let [e (decode-element input elemCodec protoVersion)]
            (recur (inc counter) (conj! result e))))))))

(defn- clj-set-codec [^DataType cqlType ^TypeCodec elemCodec]
  (let [java-type (GenericType/setOf (.getJavaType elemCodec))]
    (reify TypeCodec
      (getJavaType [_] java-type)
      (getCqlType [_] cqlType)
      (^boolean accepts [_ ^Object value]
        (if (instance? Set value)
          (if (empty? value)
            true
            (let [e (first value)]
              (.accepts ^TypeCodec elemCodec e)
          false))))
      (encode [_ val protoVersion]
        (encode-helper val protoVersion elemCodec))
      (decode [_ bytes protoVersion]
        (decode-helper bytes protoVersion elemCodec #{}))
      (format [_ value] (str value))
      (parse [_ value]  (edn/read-string value)))))

(defn- clj-list-codec [^DataType cqlType ^TypeCodec elemCodec]
  (let [java-type (GenericType/setOf (.getJavaType elemCodec))]
    (reify TypeCodec
      (getJavaType [_] java-type)
      (getCqlType [_] cqlType)
      (^boolean accepts [_ ^Object value]
        (if (instance? List value)
          (if (empty? value)
            true
            (let [e (first value)]
              (.accepts ^TypeCodec elemCodec e)
              false))))
      (encode [_ val protoVersion]
        (encode-helper val protoVersion elemCodec))
      (decode [_ bytes protoVersion]
        (decode-helper bytes protoVersion elemCodec []))
      (format [_ value] (str value))
      (parse [_ value]  (edn/read-string value)))))

(defn map-of [^TypeCodec keyCodec ^TypeCodec valueCodec]
  (let [cqlType (DataTypes/mapOf (.getCqlType keyCodec), (.getCqlType valueCodec))]
    (clj-map-codec cqlType keyCodec valueCodec)))

(defn set-of [^TypeCodec elemCodec]
  (let [cqlType (DataTypes/setOf (.getCqlType elemCodec))]
    (clj-set-codec cqlType elemCodec)))

(defn list-of [^TypeCodec elemCodec]
  (let [cqlType (DataTypes/setOf (.getCqlType elemCodec))]
    (clj-list-codec cqlType elemCodec)))

