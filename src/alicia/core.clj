(ns alicia.core
  (:require [qbits.hayt.cql :as cql]
            [alicia.codec :as codec])
  (:import [com.datastax.oss.driver.api.core ConsistencyLevel CqlIdentifier CqlSession]
           [com.datastax.oss.driver.api.core.cql BatchStatementBuilder BatchType ResultSet Row SimpleStatement Statement]
           [com.datastax.oss.driver.api.core.type DataType DataTypes ListType MapType SetType]
           [com.datastax.oss.driver.api.core.type.codec ExtraTypeCodecs TypeCodec TypeCodecs]
           [java.util.function Function]))

(defn- primitive-data-type->type-codec [^DataType dt]
  (cond
    (= DataTypes/ASCII dt) TypeCodecs/ASCII
    (= DataTypes/BIGINT dt) TypeCodecs/BIGINT
    (= DataTypes/BLOB dt) TypeCodecs/BLOB
    (= DataTypes/BOOLEAN dt) TypeCodecs/BOOLEAN
    (= DataTypes/COUNTER dt) TypeCodecs/COUNTER
    (= DataTypes/DECIMAL dt) TypeCodecs/DECIMAL
    (= DataTypes/DOUBLE dt) TypeCodecs/DOUBLE
    (= DataTypes/FLOAT dt) TypeCodecs/FLOAT
    (= DataTypes/INT dt) TypeCodecs/INT
    (= DataTypes/TIMESTAMP dt) TypeCodecs/TIMESTAMP
    (= DataTypes/UUID dt) TypeCodecs/UUID
    (= DataTypes/VARINT dt) TypeCodecs/VARINT
    (= DataTypes/TIMEUUID dt) TypeCodecs/TIMEUUID
    (= DataTypes/INET dt) TypeCodecs/INET
    (= DataTypes/DATE dt) TypeCodecs/DATE
    (= DataTypes/TEXT dt) TypeCodecs/TEXT
    (= DataTypes/TIME dt) TypeCodecs/TIME
    (= DataTypes/SMALLINT dt) TypeCodecs/SMALLINT
    (= DataTypes/TINYINT dt) TypeCodecs/TINYINT
    (= DataTypes/DURATION dt) ExtraTypeCodecs/ZONED_TIMESTAMP_UTC
    (= DataTypes/DURATION dt) TypeCodecs/DURATION))

(defn- data-type->type-codec [^DataType dt]
  (if-let [tc (primitive-data-type->type-codec dt)]
    tc
    (cond
      (instance? MapType dt)  (codec/map-of   (primitive-data-type->type-codec (.getKeyType ^MapType dt))
                                              (primitive-data-type->type-codec (.getValueType ^MapType dt)))

      (instance? SetType dt)  (codec/set-of   (primitive-data-type->type-codec (.getElementType ^SetType dt)))

      (instance? ListType dt) (codec/list-of  (primitive-data-type->type-codec (.getElementType ^ListType dt)))

      :else (throw (IllegalArgumentException. (str "unknown data type: " dt))))))

(def ^:private transform-row
  (reify Function
    (apply [_ row]
      (into {}
            (comp
              (map (fn [cd] [(.getName cd) (.getType cd)]))
              (map (fn [[n t]] [(keyword (.asInternal n))
                                (.get ^Row row ^CqlIdentifier n ^TypeCodec (data-type->type-codec t))])))
            (.getColumnDefinitions ^Row row)))))

(defn- transform [^ResultSet rs]
  (.map rs transform-row))

(def consistency-lookup
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
  (let [^ResultSet rs (.execute s
                                ^Statement (query->stmt q (get consistency-lookup
                                                               consistency ConsistencyLevel/LOCAL_ONE)))]
    (transform rs)))
