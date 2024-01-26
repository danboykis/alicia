(ns alicia.core
  (:require [qbits.hayt.cql :as cql]
            [alicia.codec :as codec])
  (:import [com.datastax.oss.driver.api.core CqlIdentifier CqlSession]
           [com.datastax.oss.driver.api.core.cql ResultSet Row SimpleStatement]
           [com.datastax.oss.driver.api.core.type DataType DataTypes ListType MapType SetType]
           [com.datastax.oss.driver.api.core.type.codec TypeCodec TypeCodecs]
           [java.util.function Function]))

(defn- primitive-data-type->type-codec [^DataType dt]
  (cond
    (= DataTypes/ASCII dt)      TypeCodecs/ASCII
    (= DataTypes/BIGINT dt)     TypeCodecs/BIGINT
    (= DataTypes/BLOB dt)       TypeCodecs/BLOB
    (= DataTypes/BOOLEAN dt)    TypeCodecs/BOOLEAN
    (= DataTypes/COUNTER dt)    TypeCodecs/COUNTER
    (= DataTypes/DECIMAL dt)    TypeCodecs/DECIMAL
    (= DataTypes/DOUBLE dt)     TypeCodecs/DOUBLE
    (= DataTypes/FLOAT dt)      TypeCodecs/FLOAT
    (= DataTypes/INT dt)        TypeCodecs/INT
    (= DataTypes/TIMESTAMP dt)  TypeCodecs/TIMESTAMP
    (= DataTypes/UUID dt)       TypeCodecs/UUID
    (= DataTypes/VARINT dt)     TypeCodecs/VARINT
    (= DataTypes/TIMEUUID dt)   TypeCodecs/TIMEUUID
    (= DataTypes/INET dt)       TypeCodecs/INET
    (= DataTypes/DATE dt)       TypeCodecs/DATE
    (= DataTypes/TEXT dt)       TypeCodecs/TEXT
    (= DataTypes/TIME dt)       TypeCodecs/TIME
    (= DataTypes/SMALLINT dt)   TypeCodecs/SMALLINT
    (= DataTypes/TINYINT dt)    TypeCodecs/TINYINT
    (= DataTypes/DURATION dt)   TypeCodecs/DURATION))

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

(defn execute! [^CqlSession s q]
  (let [q' (cond
             (map? q)     (cql/->raw q)
             (string? q)  q
             :else        (throw (IllegalArgumentException. (str "unknown query format: " (type q) " " q))))
        ^ResultSet rs (.execute s (SimpleStatement/newInstance q'))]
    (transform rs)))
