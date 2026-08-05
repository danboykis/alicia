(ns alicia.connect
  (:import [com.datastax.oss.driver.api.core CqlSession]
           [com.datastax.oss.driver.api.core.config DefaultDriverOption DriverConfigLoader]
           [java.net InetSocketAddress]
           [java.time Duration]
           [java.time.temporal ChronoUnit]))

(def ^:private driver-option-specs
  "Kebab-case option keyword -> [DefaultDriverOption kind].
  kind is :string | :name (enum-ish, keywordizable) | :int | :long | :boolean
  | :duration | :strings | :durations | :doubles | :string-map.
  Covers every value-typed DefaultDriverOption; the only enum constants not
  here are the container paths LOAD_BALANCING_POLICY, RETRY_POLICY and
  SPECULATIVE_EXECUTION_POLICY, which have no value of their own."
  {;; basic
   :contact-points           [DefaultDriverOption/CONTACT_POINTS :strings]
   :session-name             [DefaultDriverOption/SESSION_NAME :string]
   :session-keyspace         [DefaultDriverOption/SESSION_KEYSPACE :string]
   :config-reload-interval   [DefaultDriverOption/CONFIG_RELOAD_INTERVAL :duration]
   :resolve-contact-points   [DefaultDriverOption/RESOLVE_CONTACT_POINTS :boolean]
   :cloud-secure-connect-bundle
   [DefaultDriverOption/CLOUD_SECURE_CONNECT_BUNDLE :string]
   ;; basic request
   :request-consistency        [DefaultDriverOption/REQUEST_CONSISTENCY :name]
   :request-serial-consistency [DefaultDriverOption/REQUEST_SERIAL_CONSISTENCY :name]
   :request-timeout            [DefaultDriverOption/REQUEST_TIMEOUT :duration]
   :request-page-size          [DefaultDriverOption/REQUEST_PAGE_SIZE :int]
   :request-idempotent         [DefaultDriverOption/REQUEST_DEFAULT_IDEMPOTENCE :boolean]
   :request-log-warnings       [DefaultDriverOption/REQUEST_LOG_WARNINGS :boolean]
   :request-warn-if-set-keyspace
   [DefaultDriverOption/REQUEST_WARN_IF_SET_KEYSPACE :boolean]
   ;; load balancing
   :load-balancing-policy-class
   [DefaultDriverOption/LOAD_BALANCING_POLICY_CLASS :string]
   :local-datacenter
   [DefaultDriverOption/LOAD_BALANCING_LOCAL_DATACENTER :string]
   :load-balancing-filter-class ; deprecated, use distance evaluator
   [DefaultDriverOption/LOAD_BALANCING_FILTER_CLASS :string]
   :load-balancing-distance-evaluator-class
   [DefaultDriverOption/LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS :string]
   :load-balancing-policy-slow-avoidance
   [DefaultDriverOption/LOAD_BALANCING_POLICY_SLOW_AVOIDANCE :boolean]
   :load-balancing-dc-failover-max-nodes-per-remote-dc
   [DefaultDriverOption/LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC :int]
   :load-balancing-dc-failover-allow-for-local-consistency-levels
   [DefaultDriverOption/LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS :boolean]
   :load-balancing-dc-failover-preferred-remote-dcs
   [DefaultDriverOption/LOAD_BALANCING_DC_FAILOVER_PREFERRED_REMOTE_DCS :strings]
   ;; connection / pooling
   :connect-timeout             [DefaultDriverOption/CONNECTION_CONNECT_TIMEOUT :duration]
   :init-query-timeout          [DefaultDriverOption/CONNECTION_INIT_QUERY_TIMEOUT :duration]
   :set-keyspace-timeout        [DefaultDriverOption/CONNECTION_SET_KEYSPACE_TIMEOUT :duration]
   :max-requests-per-connection [DefaultDriverOption/CONNECTION_MAX_REQUESTS :int]
   :connection-max-orphan-requests
   [DefaultDriverOption/CONNECTION_MAX_ORPHAN_REQUESTS :int]
   :connection-warn-init-error [DefaultDriverOption/CONNECTION_WARN_INIT_ERROR :boolean]
   :pool-local-size            [DefaultDriverOption/CONNECTION_POOL_LOCAL_SIZE :int]
   :pool-remote-size           [DefaultDriverOption/CONNECTION_POOL_REMOTE_SIZE :int]
   :heartbeat-interval         [DefaultDriverOption/HEARTBEAT_INTERVAL :duration]
   :heartbeat-timeout          [DefaultDriverOption/HEARTBEAT_TIMEOUT :duration]
   ;; reconnection
   :reconnect-on-init        [DefaultDriverOption/RECONNECT_ON_INIT :boolean]
   :reconnection-policy-class
   [DefaultDriverOption/RECONNECTION_POLICY_CLASS :string]
   :reconnect-base-delay     [DefaultDriverOption/RECONNECTION_BASE_DELAY :duration]
   :reconnect-max-delay      [DefaultDriverOption/RECONNECTION_MAX_DELAY :duration]
   ;; retry / speculative execution
   :retry-policy-class       [DefaultDriverOption/RETRY_POLICY_CLASS :string]
   :speculative-execution-policy-class
   [DefaultDriverOption/SPECULATIVE_EXECUTION_POLICY_CLASS :string]
   :speculative-execution-max
   [DefaultDriverOption/SPECULATIVE_EXECUTION_MAX :int]
   :speculative-execution-delay
   [DefaultDriverOption/SPECULATIVE_EXECUTION_DELAY :duration]
   ;; auth
   :auth-provider-class    [DefaultDriverOption/AUTH_PROVIDER_CLASS :string]
   :auth-provider-user-name
   [DefaultDriverOption/AUTH_PROVIDER_USER_NAME :string]
   :auth-provider-password [DefaultDriverOption/AUTH_PROVIDER_PASSWORD :string]
   ;; SSL
   :ssl-engine-factory-class [DefaultDriverOption/SSL_ENGINE_FACTORY_CLASS :string]
   :ssl-cipher-suites        [DefaultDriverOption/SSL_CIPHER_SUITES :strings]
   :ssl-hostname-validation  [DefaultDriverOption/SSL_HOSTNAME_VALIDATION :boolean]
   :ssl-truststore-path      [DefaultDriverOption/SSL_TRUSTSTORE_PATH :string]
   :ssl-truststore-password  [DefaultDriverOption/SSL_TRUSTSTORE_PASSWORD :string]
   :ssl-keystore-path        [DefaultDriverOption/SSL_KEYSTORE_PATH :string]
   :ssl-keystore-password    [DefaultDriverOption/SSL_KEYSTORE_PASSWORD :string]
   :ssl-keystore-reload-interval
   [DefaultDriverOption/SSL_KEYSTORE_RELOAD_INTERVAL :duration]
   :ssl-allow-dns-reverse-lookup-san
   [DefaultDriverOption/SSL_ALLOW_DNS_REVERSE_LOOKUP_SAN :boolean]
   ;; timestamps / ids
   :timestamp-generator-class
   [DefaultDriverOption/TIMESTAMP_GENERATOR_CLASS :string]
   :timestamp-generator-force-java-clock
   [DefaultDriverOption/TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK :boolean]
   :timestamp-generator-drift-warning-threshold
   [DefaultDriverOption/TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD :duration]
   :timestamp-generator-drift-warning-interval
   [DefaultDriverOption/TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL :duration]
   :request-id-generator-class
   [DefaultDriverOption/REQUEST_ID_GENERATOR_CLASS :string]
   ;; request tracking / logging / throttling
   :request-tracker-class ; deprecated, use :request-tracker-classes
   [DefaultDriverOption/REQUEST_TRACKER_CLASS :string]
   :request-tracker-classes [DefaultDriverOption/REQUEST_TRACKER_CLASSES :strings]
   :request-logger-success-enabled
   [DefaultDriverOption/REQUEST_LOGGER_SUCCESS_ENABLED :boolean]
   :request-logger-slow-enabled
   [DefaultDriverOption/REQUEST_LOGGER_SLOW_ENABLED :boolean]
   :request-logger-slow-threshold
   [DefaultDriverOption/REQUEST_LOGGER_SLOW_THRESHOLD :duration]
   :request-logger-error-enabled
   [DefaultDriverOption/REQUEST_LOGGER_ERROR_ENABLED :boolean]
   :request-logger-max-query-length
   [DefaultDriverOption/REQUEST_LOGGER_MAX_QUERY_LENGTH :int]
   :request-logger-values  [DefaultDriverOption/REQUEST_LOGGER_VALUES :boolean]
   :request-logger-max-value-length
   [DefaultDriverOption/REQUEST_LOGGER_MAX_VALUE_LENGTH :int]
   :request-logger-max-values
   [DefaultDriverOption/REQUEST_LOGGER_MAX_VALUES :int]
   :request-logger-stack-traces
   [DefaultDriverOption/REQUEST_LOGGER_STACK_TRACES :boolean]
   :request-throttler-class [DefaultDriverOption/REQUEST_THROTTLER_CLASS :string]
   :request-throttler-max-concurrent-requests
   [DefaultDriverOption/REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS :int]
   :request-throttler-max-requests-per-second
   [DefaultDriverOption/REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND :int]
   :request-throttler-max-queue-size
   [DefaultDriverOption/REQUEST_THROTTLER_MAX_QUEUE_SIZE :int]
   :request-throttler-drain-interval
   [DefaultDriverOption/REQUEST_THROTTLER_DRAIN_INTERVAL :duration]
   ;; request tracing
   :request-trace-attempts    [DefaultDriverOption/REQUEST_TRACE_ATTEMPTS :int]
   :request-trace-interval    [DefaultDriverOption/REQUEST_TRACE_INTERVAL :duration]
   :request-trace-consistency [DefaultDriverOption/REQUEST_TRACE_CONSISTENCY :name]
   ;; metadata listeners
   :metadata-node-state-listener-class ; deprecated, use plural variant
   [DefaultDriverOption/METADATA_NODE_STATE_LISTENER_CLASS :string]
   :metadata-schema-change-listener-class ; deprecated, use plural variant
   [DefaultDriverOption/METADATA_SCHEMA_CHANGE_LISTENER_CLASS :string]
   :metadata-node-state-listener-classes
   [DefaultDriverOption/METADATA_NODE_STATE_LISTENER_CLASSES :strings]
   :metadata-schema-change-listener-classes
   [DefaultDriverOption/METADATA_SCHEMA_CHANGE_LISTENER_CLASSES :strings]
   ;; address translation
   :address-translator-class
   [DefaultDriverOption/ADDRESS_TRANSLATOR_CLASS :string]
   :address-translator-advertised-hostname
   [DefaultDriverOption/ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME :string]
   :address-translator-subnet-addresses
   [DefaultDriverOption/ADDRESS_TRANSLATOR_SUBNET_ADDRESSES :string-map]
   :address-translator-default-address
   [DefaultDriverOption/ADDRESS_TRANSLATOR_DEFAULT_ADDRESS :string]
   :address-translator-resolve-addresses
   [DefaultDriverOption/ADDRESS_TRANSLATOR_RESOLVE_ADDRESSES :boolean]
   ;; protocol
   :protocol-version          [DefaultDriverOption/PROTOCOL_VERSION :string]
   :protocol-compression      [DefaultDriverOption/PROTOCOL_COMPRESSION :string]
   :protocol-max-frame-length [DefaultDriverOption/PROTOCOL_MAX_FRAME_LENGTH :long]
   ;; metrics
   :metrics-session-enabled [DefaultDriverOption/METRICS_SESSION_ENABLED :strings]
   :metrics-node-enabled    [DefaultDriverOption/METRICS_NODE_ENABLED :strings]
   :metrics-generate-aggregable-histograms
   [DefaultDriverOption/METRICS_GENERATE_AGGREGABLE_HISTOGRAMS :boolean]
   :metrics-session-cql-requests-highest
   [DefaultDriverOption/METRICS_SESSION_CQL_REQUESTS_HIGHEST :duration]
   :metrics-session-cql-requests-lowest
   [DefaultDriverOption/METRICS_SESSION_CQL_REQUESTS_LOWEST :duration]
   :metrics-session-cql-requests-slo
   [DefaultDriverOption/METRICS_SESSION_CQL_REQUESTS_SLO :durations]
   :metrics-session-cql-requests-publish-percentiles
   [DefaultDriverOption/METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES :doubles]
   :metrics-session-cql-requests-digits
   [DefaultDriverOption/METRICS_SESSION_CQL_REQUESTS_DIGITS :int]
   :metrics-session-cql-requests-interval
   [DefaultDriverOption/METRICS_SESSION_CQL_REQUESTS_INTERVAL :duration]
   :metrics-session-throttling-highest
   [DefaultDriverOption/METRICS_SESSION_THROTTLING_HIGHEST :duration]
   :metrics-session-throttling-lowest
   [DefaultDriverOption/METRICS_SESSION_THROTTLING_LOWEST :duration]
   :metrics-session-throttling-slo
   [DefaultDriverOption/METRICS_SESSION_THROTTLING_SLO :durations]
   :metrics-session-throttling-publish-percentiles
   [DefaultDriverOption/METRICS_SESSION_THROTTLING_PUBLISH_PERCENTILES :doubles]
   :metrics-session-throttling-digits
   [DefaultDriverOption/METRICS_SESSION_THROTTLING_DIGITS :int]
   :metrics-session-throttling-interval
   [DefaultDriverOption/METRICS_SESSION_THROTTLING_INTERVAL :duration]
   :metrics-node-cql-messages-highest
   [DefaultDriverOption/METRICS_NODE_CQL_MESSAGES_HIGHEST :duration]
   :metrics-node-cql-messages-lowest
   [DefaultDriverOption/METRICS_NODE_CQL_MESSAGES_LOWEST :duration]
   :metrics-node-cql-messages-slo
   [DefaultDriverOption/METRICS_NODE_CQL_MESSAGES_SLO :durations]
   :metrics-node-cql-messages-publish-percentiles
   [DefaultDriverOption/METRICS_NODE_CQL_MESSAGES_PUBLISH_PERCENTILES :doubles]
   :metrics-node-cql-messages-digits
   [DefaultDriverOption/METRICS_NODE_CQL_MESSAGES_DIGITS :int]
   :metrics-node-cql-messages-interval
   [DefaultDriverOption/METRICS_NODE_CQL_MESSAGES_INTERVAL :duration]
   :metrics-node-expire-after
   [DefaultDriverOption/METRICS_NODE_EXPIRE_AFTER :duration]
   :metrics-factory-class [DefaultDriverOption/METRICS_FACTORY_CLASS :string]
   :metrics-id-generator-class
   [DefaultDriverOption/METRICS_ID_GENERATOR_CLASS :string]
   :metrics-id-generator-prefix
   [DefaultDriverOption/METRICS_ID_GENERATOR_PREFIX :string]
   ;; socket
   :socket-tcp-nodelay         [DefaultDriverOption/SOCKET_TCP_NODELAY :boolean]
   :socket-keep-alive          [DefaultDriverOption/SOCKET_KEEP_ALIVE :boolean]
   :socket-reuse-address       [DefaultDriverOption/SOCKET_REUSE_ADDRESS :boolean]
   :socket-linger-interval     [DefaultDriverOption/SOCKET_LINGER_INTERVAL :int]
   :socket-receive-buffer-size [DefaultDriverOption/SOCKET_RECEIVE_BUFFER_SIZE :int]
   :socket-send-buffer-size    [DefaultDriverOption/SOCKET_SEND_BUFFER_SIZE :int]
   ;; metadata / schema
   :metadata-topology-window   [DefaultDriverOption/METADATA_TOPOLOGY_WINDOW :duration]
   :metadata-topology-max-events
   [DefaultDriverOption/METADATA_TOPOLOGY_MAX_EVENTS :int]
   :metadata-schema-enabled [DefaultDriverOption/METADATA_SCHEMA_ENABLED :boolean]
   :metadata-schema-request-timeout
   [DefaultDriverOption/METADATA_SCHEMA_REQUEST_TIMEOUT :duration]
   :metadata-schema-request-page-size
   [DefaultDriverOption/METADATA_SCHEMA_REQUEST_PAGE_SIZE :int]
   :metadata-schema-refreshed-keyspaces
   [DefaultDriverOption/METADATA_SCHEMA_REFRESHED_KEYSPACES :strings]
   :metadata-schema-window [DefaultDriverOption/METADATA_SCHEMA_WINDOW :duration]
   :metadata-schema-max-events
   [DefaultDriverOption/METADATA_SCHEMA_MAX_EVENTS :int]
   :metadata-token-map-enabled
   [DefaultDriverOption/METADATA_TOKEN_MAP_ENABLED :boolean]
   ;; control connection
   :control-connection-timeout
   [DefaultDriverOption/CONTROL_CONNECTION_TIMEOUT :duration]
   :control-connection-agreement-interval
   [DefaultDriverOption/CONTROL_CONNECTION_AGREEMENT_INTERVAL :duration]
   :control-connection-agreement-timeout
   [DefaultDriverOption/CONTROL_CONNECTION_AGREEMENT_TIMEOUT :duration]
   :control-connection-agreement-warn
   [DefaultDriverOption/CONTROL_CONNECTION_AGREEMENT_WARN :boolean]
   ;; prepared statements
   :prepare-on-all-nodes         [DefaultDriverOption/PREPARE_ON_ALL_NODES :boolean]
   :reprepare-enabled            [DefaultDriverOption/REPREPARE_ENABLED :boolean]
   :reprepare-check-system-table [DefaultDriverOption/REPREPARE_CHECK_SYSTEM_TABLE :boolean]
   :reprepare-max-statements     [DefaultDriverOption/REPREPARE_MAX_STATEMENTS :int]
   :reprepare-max-parallelism    [DefaultDriverOption/REPREPARE_MAX_PARALLELISM :int]
   :reprepare-timeout            [DefaultDriverOption/REPREPARE_TIMEOUT :duration]
   :prepared-cache-weak-values   [DefaultDriverOption/PREPARED_CACHE_WEAK_VALUES :boolean]
   ;; session lifecycle
   :session-leak-threshold [DefaultDriverOption/SESSION_LEAK_THRESHOLD :int]
   ;; netty
   :netty-io-size                 [DefaultDriverOption/NETTY_IO_SIZE :int]
   :netty-io-shutdown-quiet-period
   [DefaultDriverOption/NETTY_IO_SHUTDOWN_QUIET_PERIOD :int]
   :netty-io-shutdown-timeout [DefaultDriverOption/NETTY_IO_SHUTDOWN_TIMEOUT :int]
   :netty-io-shutdown-unit    [DefaultDriverOption/NETTY_IO_SHUTDOWN_UNIT :name]
   :netty-admin-size          [DefaultDriverOption/NETTY_ADMIN_SIZE :int]
   :netty-admin-shutdown-quiet-period
   [DefaultDriverOption/NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD :int]
   :netty-admin-shutdown-timeout
   [DefaultDriverOption/NETTY_ADMIN_SHUTDOWN_TIMEOUT :int]
   :netty-admin-shutdown-unit [DefaultDriverOption/NETTY_ADMIN_SHUTDOWN_UNIT :name]
   :netty-timer-tick-duration [DefaultDriverOption/NETTY_TIMER_TICK_DURATION :duration]
   :netty-timer-ticks-per-wheel
   [DefaultDriverOption/NETTY_TIMER_TICKS_PER_WHEEL :int]
   :netty-daemon [DefaultDriverOption/NETTY_DAEMON :boolean]
   ;; coalescer
   :coalescer-max-runs ; deprecated
   [DefaultDriverOption/COALESCER_MAX_RUNS :int]
   :coalescer-interval [DefaultDriverOption/COALESCER_INTERVAL :duration]})

(def ^:private time-units
  {:nanos   ChronoUnit/NANOS
   :micros  ChronoUnit/MICROS
   :millis  ChronoUnit/MILLIS
   :seconds ChronoUnit/SECONDS
   :minutes ChronoUnit/MINUTES
   :hours   ChronoUnit/HOURS
   :days    ChronoUnit/DAYS})

(defn- ->duration
  "Coerces a Duration | millis number | [n unit-kw] | ISO-8601 string to a Duration."
  ^Duration [v]
  (cond
    (instance? Duration v) v
    (number? v)            (Duration/ofMillis (long v))
    (vector? v)            (let [[n unit] v]
                             (if-some [u (time-units unit)]
                               (Duration/of (long n) u)
                               (throw (ex-info "unknown time unit" {:unit unit :valid (keys time-units)}))))
    (string? v)            (Duration/parse v)
    :else                  (throw (ex-info "not a duration" {:value v}))))

(defn- ->quoted-key
  "Quotes a config map key for HOCON path syntax so dots stay literal
  (e.g. \"10.0.0.0/24\" must not be split into path segments)."
  ^String [k]
  (str \" (-> (str k) (.replace "\\" "\\\\") (.replace "\"" "\\\"")) \"))

(defn- ->enum-name
  ":local-quorum -> \"LOCAL_QUORUM\"; strings pass through."
  ^String [v]
  (if (keyword? v) (-> (name v) (.replace \- \_) .toUpperCase) (str v)))

(defn- config-loader
  "Builds a DriverConfigLoader from a map of kebab-case option keywords, e.g.

    {:local-datacenter      \"dc1\"
     :request-consistency   :local-quorum
     :request-timeout       [3 :seconds]   ; also: millis number, Duration, \"PT3S\"
     :ssl-hostname-validation true}

  Unknown keys throw ex-info listing the valid options."
  ^DriverConfigLoader [m]
  (let [b (DriverConfigLoader/programmaticBuilder)]
    (doseq [[k v] m]
      (let [[^DefaultDriverOption opt kind]
            (or (driver-option-specs k)
                (throw (ex-info (str "unknown driver option: " k)
                                {:key k :valid (sort (keys driver-option-specs))})))]
        (case kind
          :string     (.withString b opt (str v))
          :name       (.withString b opt (->enum-name v))
          :duration   (.withDuration b opt (->duration v))
          :int        (.withInt b opt (int v))
          :long       (.withLong b opt (long v))
          :boolean    (.withBoolean b opt (boolean v))
          :strings    (.withStringList b opt (mapv str v))
          :durations  (.withDurationList b opt (mapv ->duration v))
          :doubles    (.withDoubleList b opt (mapv double v))
          :string-map (.withStringMap b opt (into {} (map (fn [[k v]] [(->quoted-key k) (str v)])) v)))))
    (.build b)))

(def ^:private default-remote-config
  "Sane baselines for a remote cluster; merge user config over this."
  {:request-consistency      :local-one
   :request-timeout          [3 :seconds]
   :request-idempotent       true
   :connect-timeout          [5 :seconds]
   :init-query-timeout       [5 :seconds]
   :set-keyspace-timeout     [5 :seconds]
   :pool-local-size          1
   :reconnect-base-delay     [500 :millis]
   :reconnect-max-delay      [30 :seconds]
   :heartbeat-interval       [30 :seconds]
   :heartbeat-timeout        [5 :seconds]})

(defn connect-remote!
  [{:keys [username password keyspace hosts port config]
    :or {port 9042}}]
  (let [cfg (merge default-remote-config config)]
    (when-not (:local-datacenter cfg)
      (throw (ex-info "remote config requires :local-datacenter" {:config cfg})))
    {:session
     (-> (CqlSession/builder)
         (.withConfigLoader (config-loader cfg))
         (.addContactPoints (mapv #(InetSocketAddress. ^String % (int port)) hosts))
         (.withAuthCredentials ^String username ^String password)
         (.withKeyspace ^String keyspace)
         .build)}))
