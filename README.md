# alicia

> Alicia Dominica, known as the "Patron Saint of the Sisterhood," "Bearer of the Grail of Ages," and "Founder of the Order of the Ebon Chalice," is revered throughout the Imperium of Man as both the founder and patron Imperial Saint of the Adepta Sororitas.

Alicia is a _very_ light wrapper around `com.datastax.oss/java-driver-core` driver.
It provides a little bit of functionality to make Cassandra interactions a little friendlier in Clojure.

## Example Usage

Initialize `CqlSession` object somehow, for example like this:

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

Perform a query using [hayt](https://github.com/mpenet/hayt/)

```clojure
(require '[alicia.core :as ac])
(def my-config-map {.......})
(def cql-session (connect! my-config-map))
(first (ac/execute! cql-session {:select :system.local :columns :* :limit 1}))

{:broadcast_port 7000,
 :key "local",
 :cql_version "3.4.6",
 :schema_version #uuid"54e17321-3f2e-37ca-9b08-d91ba7bdd369",
 :gossip_generation 1706191471,
 :bootstrapped "COMPLETED",
 :host_id #uuid"1ad89b8f-920c-4e2d-b7ca-736fef87c780",
 :rpc_port 9042,
 :data_center "datacenter1",
 :release_version "4.1.3",
 :broadcast_address #object[java.net.Inet4Address 0x5f76c5fc "/10.89.0.3"],
 :listen_port 7000,
 :listen_address #object[java.net.Inet4Address 0x365bb5a1 "/10.89.0.3"],
 :truncated_at {#uuid"176c39cd-b93d-33a5-a218-8eb06a56f66e" #object[java.nio.HeapByteBuffer
                                                                    0x374b35df
                                                                    "java.nio.HeapByteBuffer[pos=0 lim=20 cap=108]"],
                #uuid"618f817b-005f-3678-b8a4-53f3930b8e86" #object[java.nio.HeapByteBuffer
                                                                    0x37179c7c
                                                                    "java.nio.HeapByteBuffer[pos=0 lim=20 cap=64]"],
                #uuid"62efe31f-3be8-310c-8d29-8963439c1288" #object[java.nio.HeapByteBuffer
                                                                    0x5ea7f647
                                                                    "java.nio.HeapByteBuffer[pos=0 lim=20 cap=20]"]},
 :rpc_address #object[java.net.Inet4Address 0x353ab68f "/10.89.0.3"],
 :cluster_name "Test Cluster",
 :partitioner "org.apache.cassandra.dht.Murmur3Partitioner",
 :native_protocol_version "5",
 :tokens #{"-112218665982246025"
           "-1183719282322278837"
           "-2623987512224862077"
           "-3468364159716023833"
           "-4559661270408236093"
           "-5655703170643174382"
           "-7434337471466761511"
           "-8412482923982875587"
           "1723304671372531784"
           "2799060552598111323"
           "3534976245309004964"
           "4614349052756606622"
           "5857869058437198594"
           "6825233301515734093"
           "8415237854029632731"
           "905632738760180990"},
 :rack "rack1"}
```