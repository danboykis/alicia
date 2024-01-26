(defproject com.danboykis/alicia "0.0.1-SNAPSHOT"
  :description "Clojure facade for Cassandra"
  :url "http://github.com/danboykis/alicia"
  :license {:name "Unlicense"
            :url "https://unlicense.org/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.datastax.oss/java-driver-core "4.17.0"]
                 [cc.qbits/hayt "4.1.0"]]
  :profiles {:dev {:source-paths ["dev"]
                   :repl-options {:init-ns user}
                   :dependencies [[com.danboykis/abk "0.1.3-SNAPSHOT"]]}}
  :repl-options {:init-ns alicia.core})
