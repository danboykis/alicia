# alicia

> Alicia Dominica, known as the "Patron Saint of the Sisterhood," "Bearer of the Grail of Ages," and "Founder of the Order of the Ebon Chalice," is revered throughout the Imperium of Man as both the founder and patron Imperial Saint of the Adepta Sororitas.

Alicia is a *very* light wrapper around the Cassandra Java driver 4.x
(`java-driver-core`). It provides a little bit of functionality to make
Cassandra interactions a little friendlier in Clojure:

- Queries are written as Clojure data with [hayt](https://github.com/mpenet/hayt/) (raw CQL strings work too)
- Results come back as vectors of plain Clojure maps with keyword keys
- CQL collections (list/set/map) are deeply decoded into Clojure persistent vectors/sets/maps

## Installation

Leiningen (`project.clj`):

```clojure
[com.danboykis/alicia "0.0.1-SNAPSHOT"]
```

deps.edn:

```clojure
com.danboykis/alicia {:mvn/version "0.0.1-SNAPSHOT"}
```

## Usage

### Connecting

Alicia doesn't manage connections — build a `CqlSession` yourself and pass it
in. For example:

```clojure
(defn connect! [m]
  (let [{{username :username password :password} :credentials
         keyspace :keyspace hosts :hosts port :port dc :dc driver-conf :driver-config} m
        ^CqlSession cql-session (-> (CqlSession/builder)
                                    (.withConfigLoader (DriverConfigLoader/fromString driver-conf))
                                    (.withAuthCredentials username password)
                                    (.addContactEndPoints (into []
                                                                (map #(DefaultEndPoint. (InetSocketAddress. ^String % ^int port)))
                                                                hosts))
                                    (.withLocalDatacenter dc)
                                    (.withKeyspace ^String keyspace)
                                    (.build))]
    cql-session))
```

### Queries

Perform a query using a [hayt](https://github.com/mpenet/hayt/) map:

```clojure
(require '[alicia.core :as ac])

(def cql-session (connect! my-config-map))

(first (ac/execute! cql-session {:select :system.local :columns :* :limit 1}))
;; =>
{:key "local"
 :cluster_name "Test Cluster"
 :release_version "4.1.3"
 :data_center "datacenter1"
 :rack "rack1"
 :host_id #uuid "1ad89b8f-920c-4e2d-b7ca-736fef87c780"
 :partitioner "org.apache.cassandra.dht.Murmur3Partitioner"
 :tokens #{"-112218665982246025" "-1183719282322278837" ...}
 ...}
```

Raw CQL strings are also accepted:

```clojure
(ac/execute! cql-session "select * from system.local limit 1")
```

### Inserts

Given the following schema:

```sql
CREATE KEYSPACE foobar
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };

CREATE TABLE foobar.example_table (
    id UUID PRIMARY KEY,
    rank int,
    birthdate timestamp,
    lastname text,
    firstname text);
```

Write rows with hayt `:insert` maps:

```clojure
(ac/execute! cql-session {:insert :foobar.example_table
                          :values {:id (UUID/randomUUID)
                                   :rank 11
                                   :firstname "Dan"
                                   :lastname "Brown"
                                   :birthdate (.toEpochMilli (Instant/now))}})
```

### Batched writes

Wrap multiple queries in `{:batch <type> :queries [...]}`, where `<type>` is
`:logged`, `:unlogged`, or `:counter`:

```clojure
(ac/execute! cql-session
             {:batch :logged
              :queries [{:insert :foobar.example_table
                         :values {:id (UUID/randomUUID) :rank 11 :firstname "Dan" :lastname "Brown"
                                  :birthdate (.toEpochMilli (Instant/now))}}
                        {:insert :foobar.example_table
                         :values {:id (UUID/randomUUID) :rank 10 :firstname "John" :lastname "Brown"
                                  :birthdate (.toEpochMilli (Instant/now))}}
                        {:insert :foobar.example_table
                         :values {:id (UUID/randomUUID) :rank 9 :firstname "George" :lastname "Brown"
                                  :birthdate (.toEpochMilli (Instant/now))}}]})
```

### Consistency level

Pass `:consistency` as a keyword option to `execute!` (defaults to `:local-one`):

```clojure
(ac/execute! cql-session {:select :foobar.example_table :columns :*} :consistency :quorum)
```

Valid values: `:any`, `:one`, `:two`, `:three`, `:quorum`, `:all`,
`:local-one`, `:local-quorum`, `:each-quorum`, `:serial`, `:local-serial`.

## License

Released under the [Unlicense](https://unlicense.org/). See [LICENSE](LICENSE).
