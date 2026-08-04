(ns alicia.core
  (:require [qbits.hayt.cql :as cql]
            [alicia.codec :as codec])
  (:import [com.datastax.oss.driver.api.core ConsistencyLevel CqlIdentifier CqlSession]
           [com.datastax.oss.driver.api.core.context DriverContext]
           [com.datastax.oss.driver.api.core.cql BatchStatementBuilder BatchType ColumnDefinition ResultSet Row SimpleStatement Statement]
           [com.datastax.oss.driver.api.core.type.codec TypeCodec]
           [com.datastax.oss.driver.api.core.type.codec.registry CodecRegistry]))

(defn- column-spec
  "Returns [key codec identifier] for one column of a result set. The codec is
  resolved from the session's codec registry (so user-registered codecs are
  honored and tuples/UDTs work) and adapted to return Clojure collections."
  [^CodecRegistry registry ^ColumnDefinition cd]
  (let [cql-type (.getType cd)
        id       (.getName cd)]
    [(keyword (.asInternal id))
     (codec/clojure-codec cql-type (.codecFor registry cql-type))
     id]))

(defn- transform
  "Maps each row of rs to a Clojure map. The column
  codecs are computed once per result set rather than once per row."
  [^CodecRegistry registry ^ResultSet rs]
  (let [cols (mapv #(column-spec registry %) (.getColumnDefinitions rs))
        process-row (fn [^Row row]
                      (into {}
                            (map (fn [[k ^TypeCodec tc ^CqlIdentifier id]]
                                   [k (.get row id tc)]))
                            cols))]
    (into [] (map process-row) rs)))

(def ^:private consistency-lookup
  {:any ConsistencyLevel/ANY
   :one ConsistencyLevel/ONE
   :two ConsistencyLevel/TWO
   :three ConsistencyLevel/THREE
   :quorum ConsistencyLevel/QUORUM
   :all ConsistencyLevel/ALL
   :local-one ConsistencyLevel/LOCAL_ONE
   :local-quorum ConsistencyLevel/LOCAL_QUORUM
   :each-quorum ConsistencyLevel/EACH_QUORUM
   :serial ConsistencyLevel/SERIAL
   :local-serial ConsistencyLevel/LOCAL_SERIAL})

(defn- simple-query? [query]
  (or (map? query)
      (string? query)))

(defn- ^Statement query->stmt [query consistency]
  (cond
    (map? query)
    (.setConsistencyLevel (SimpleStatement/newInstance (cql/->raw query)) consistency)

    (string? query)
    (.setConsistencyLevel (SimpleStatement/newInstance query) consistency)

    (coll? query)
    (let [stmt-builder (BatchStatementBuilder. BatchType/LOGGED)]
      (doseq [q query]
        (.addStatement stmt-builder (if (simple-query? q)
                                      (query->stmt q consistency)
                                      (throw (ex-info "invalid nested batch query" {:query query})))))
      (.setConsistencyLevel stmt-builder consistency)
      (.build stmt-builder))

    :else
    (throw (IllegalArgumentException. (str "unknown query format: " (type query) " " query)))))

(defn execute! [^CqlSession s q & {:keys [consistency]}]
  (let [^ResultSet rs (.execute s (query->stmt q (get consistency-lookup
                                                      consistency ConsistencyLevel/LOCAL_ONE)))]
    (transform (.getCodecRegistry ^DriverContext (.getContext s)) rs)))
