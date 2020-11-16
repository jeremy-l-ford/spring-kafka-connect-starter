/*
 * Copyright 2019 Jeremy Ford
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeremylford.spring.kafkaconnect.configuration;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putDouble;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putInteger;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putList;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putLong;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putShort;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putString;

/**
 * PropertySupport defining a distributed kafka connect setup.
 */
public class DistributedProperties {

    private boolean enabled;

    /**
     * A unique string that identifies the Connect cluster group this worker belongs to.
     */
    private String groupId;
    /**
     * The timeout used to detect worker failures.
     * "The worker sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are
     * "received by the broker before the expiration of this session timeout, then the broker will remove the
     * "worker from the group and initiate a rebalance. Note that the value must be in the allowable range as
     * "configured in the broker configuration by <code>group.min.session.timeout.ms</code>
     * "and <code>group.max.session.timeout.ms</code>.
     */
    private Long sessionTimeoutMs;

    /**
     * The expected time between heartbeats to the group
     * coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the
     * worker's session stays active and to facilitate rebalancing when new members join or leave the group.
     * The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher
     * than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.
     */
    private Long heartbeatIntervalMs;

    /**
     * The maximum allowed time for each worker to join the group
     * once a rebalance has begun. This is basically a limit on the amount of time needed for all tasks to
     * flush any pending data and commit offsets. If the timeout is exceeded, then the worker will be removed
     * from the group, which will cause offset commit failures.
     */
    private Long rebalanceTimeoutMs;

    /**
     * When the worker is out of sync with other workers and needs
     * to resynchronize configurations, wait up to this amount of time before giving up, leaving the group, and
     * waiting a backoff period before rejoining.
     */
    private Long workerSyncTimeoutMs;

    /**
     * When the worker is out of sync with other workers and
     * fails to catch up within worker.sync.timeout.ms, leave the Connect cluster for this long before rejoining.
     */
    private Long workerUnsyncBackoffMs = (long) DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT;
    /**
     * The name of the Kafka topic where connector offsets are stored.
     */
    private String offsetStorageTopic;

    /**
     * The number of partitions used when creating the offset storage topic.
     */
    private Integer offsetStoragePartitions;

    /**
     * Replication factor used when creating the offset storage topic.
     */
    private Integer offsetStorageReplicationFactor;

    /**
     * The name of the Kafka topic where connector configurations are stored.
     */
    private String configTopic;

    /**
     * Replication factor used when creating the configuration storage topic.
     */
    private Integer configStorageReplicationFactor;

    /**
     * The name of the Kafka topic where connector and task status are stored.
     */
    private String statusStorageTopic;

    /**
     * The number of partitions used when creating the status storage topic.
     */
    private Integer statusStoragePartitions;

    /**
     * Replication factor used when creating the status storage topic.
     */
    private Integer statusStorageReplicationFactor;

    /**
     * Compatibility mode for Kafka Connect Protocol
     */
    private ConnectProtocolCompatibility connectProtocol =
            ConnectProtocolCompatibility.fromProtocol(DistributedConfig.CONNECT_PROTOCOL_DEFAULT);

    /**
     *
     */
    private Integer scheduledRebalanceMaxDelayMs = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;

    /**
     * The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any
     * partition leadership changes to proactively discover any new brokers or partitions.
     */
    private Long metadataMaxAge = 50 * 60 * 1000L;

    private Ssl ssl = new Ssl();

    private Sasl sasl = new Sasl();

    private InterWorker interWorker = new InterWorker();

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Long getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(Long sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public Long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(Long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public Long getRebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    public void setRebalanceTimeoutMs(Long rebalanceTimeoutMs) {
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
    }

    public Long getWorkerSyncTimeoutMs() {
        return workerSyncTimeoutMs;
    }

    public void setWorkerSyncTimeoutMs(Long workerSyncTimeoutMs) {
        this.workerSyncTimeoutMs = workerSyncTimeoutMs;
    }

    public Long getWorkerUnsyncBackoffMs() {
        return workerUnsyncBackoffMs;
    }

    public void setWorkerUnsyncBackoffMs(Long workerUnsyncBackoffMs) {
        this.workerUnsyncBackoffMs = workerUnsyncBackoffMs;
    }

    public String getOffsetStorageTopic() {
        return offsetStorageTopic;
    }

    public void setOffsetStorageTopic(String offsetStorageTopic) {
        this.offsetStorageTopic = offsetStorageTopic;
    }

    public Integer getOffsetStoragePartitions() {
        return offsetStoragePartitions;
    }

    public void setOffsetStoragePartitions(Integer offsetStoragePartitions) {
        this.offsetStoragePartitions = offsetStoragePartitions;
    }

    public Integer getOffsetStorageReplicationFactor() {
        return offsetStorageReplicationFactor;
    }

    public void setOffsetStorageReplicationFactor(Integer offsetStorageReplicationFactor) {
        this.offsetStorageReplicationFactor = offsetStorageReplicationFactor;
    }

    public String getConfigTopic() {
        return configTopic;
    }

    public void setConfigTopic(String configTopic) {
        this.configTopic = configTopic;
    }

    public Integer getConfigStorageReplicationFactor() {
        return configStorageReplicationFactor;
    }

    public void setConfigStorageReplicationFactor(Integer configStorageReplicationFactor) {
        this.configStorageReplicationFactor = configStorageReplicationFactor;
    }

    public String getStatusStorageTopic() {
        return statusStorageTopic;
    }

    public void setStatusStorageTopic(String statusStorageTopic) {
        this.statusStorageTopic = statusStorageTopic;
    }

    public Integer getStatusStoragePartitions() {
        return statusStoragePartitions;
    }

    public void setStatusStoragePartitions(Integer statusStoragePartitions) {
        this.statusStoragePartitions = statusStoragePartitions;
    }

    public Integer getStatusStorageReplicationFactor() {
        return statusStorageReplicationFactor;
    }

    public void setStatusStorageReplicationFactor(Integer statusStorageReplicationFactor) {
        this.statusStorageReplicationFactor = statusStorageReplicationFactor;
    }

    public Long getMetadataMaxAge() {
        return metadataMaxAge;
    }

    public void setMetadataMaxAge(Long metadataMaxAge) {
        this.metadataMaxAge = metadataMaxAge;
    }

    public ConnectProtocolCompatibility getConnectProtocol() {
        return connectProtocol;
    }

    public void setConnectProtocol(ConnectProtocolCompatibility connectProtocol) {
        this.connectProtocol = connectProtocol;
    }

    public Integer getScheduledRebalanceMaxDelayMs() {
        return scheduledRebalanceMaxDelayMs;
    }

    public void setScheduledRebalanceMaxDelayMs(Integer scheduledRebalanceMaxDelayMs) {
        this.scheduledRebalanceMaxDelayMs = scheduledRebalanceMaxDelayMs;
    }

    public Ssl getSsl() {
        return ssl;
    }

    public void setSsl(Ssl ssl) {
        this.ssl = ssl;
    }

    public Sasl getSasl() {
        return sasl;
    }

    public void setSasl(Sasl sasl) {
        this.sasl = sasl;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, String> buildProperties() {
        Properties properties = new Properties();

        putString(properties, DistributedConfig.GROUP_ID_CONFIG, groupId);
        putLong(properties, DistributedConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        putLong(properties, DistributedConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        putLong(properties, DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG, rebalanceTimeoutMs);
        putLong(properties, DistributedConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG, workerSyncTimeoutMs);
        putLong(properties, DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG, workerUnsyncBackoffMs);
        putString(properties, DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, offsetStorageTopic);
        putInteger(properties, DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, offsetStoragePartitions);
        putInteger(properties, DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, offsetStorageReplicationFactor);
        putString(properties, DistributedConfig.CONFIG_TOPIC_CONFIG, configTopic);
        putInteger(properties, DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, configStorageReplicationFactor);
        putString(properties, DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, statusStorageTopic);
        putInteger(properties, DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, statusStoragePartitions);
        putInteger(properties, DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, statusStorageReplicationFactor);
        putLong(properties, CommonClientConfigs.METADATA_MAX_AGE_CONFIG, metadataMaxAge);
        putString(properties, DistributedConfig.CONNECT_PROTOCOL_CONFIG, connectProtocol.protocol());
        putInteger(properties, DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, scheduledRebalanceMaxDelayMs);

        properties.putAll(ssl.buildProperties());
        properties.putAll(sasl.buildProperties());
        properties.putAll(interWorker.buildProperties());

        return properties;
    }

    public static class InterWorker {
        /**
         * The algorithm to use for generating internal request keys
         */
        private String keyAlgorithm = DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT;

        /**
         * The TTL of generated session keys used for internal request validation (in milliseconds)
         */
        private int keyTtlMills = DistributedConfig.INTER_WORKER_KEY_TTL_MS_MS_DEFAULT;

        /**
         * The algorithm used to sign internal requests
         */
        private String signatureAlgorithm = DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT;

        /**
         *  A list of permitted algorithms for verifying internal requests.
         */
        private List<String> verificationAlgorithm = new ArrayList<String>() {{
            add(DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT);
        }};

        public Map<String, String> buildProperties() {
            Map<String, String> properties = new HashMap<>();

            putString(properties, DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, keyAlgorithm);
            putInteger(properties, DistributedConfig.INTER_WORKER_KEY_TTL_MS_CONFIG, keyTtlMills);
            putString(properties, DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG, signatureAlgorithm);
            putList(properties, DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, verificationAlgorithm);

            return properties;
        }
    }

    //also in spring-kafka
    public static class Ssl {

        /**
         * The SSL protocol used to generate the SSLContext.
         * Default setting is TLS, which is fine for most cases.
         * Allowed values in recent JVMs are TLS, TLSv1.1 and TLSv1.2. SSL, SSLv2 and SSLv3
         * may be supported in older JVMs, but their usage is discouraged due to known security vulnerabilities.
         */
        private String protocol = SslConfigs.DEFAULT_SSL_PROTOCOL;

        /**
         * The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.
         */
        private String provider;

        /**
         * A list of cipher suites. This is a named combination of authentication, encryption,
         * MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol.
         * By default all the available cipher suites are supported.
         */
        private List<String> cipherSuites = new ArrayList<>();

        /**
         * The list of protocols enabled for SSL connections.
         */
        private List<String> enabledProtocols = Lists.newArrayList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split(","));

        /**
         * The file format of the key store file.
         * This is optional for client.
         */
        private String keyStoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE;

        /**
         * The location of the key store file.
         * This is optional for client and can be used for two-way authentication for client.
         */
        private FileSystemResource keystoreLocation;

        /**
         * The store password for the key store file.
         * This is optional for client and only needed if ssl.keystore.location is configured.
         */
        private String keystorePassword;

        /**
         * The password of the private key in the key store file.
         * This is optional for client.
         */
        private String keyPassword;

        /**
         * The file format of the trust store file.
         */
        private String trustStoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;

        /**
         * The location of the trust store file.
         */
        private FileSystemResource truststoreLocation;

        /**
         * The password for the trust store file. If a password is not set access to the truststore is still available,
         * but integrity checking is disabled.
         */
        private String truststorePassword;

        /**
         * The algorithm used by key manager factory for SSL connections.
         * Default value is the key manager factory algorithm configured for the Java Virtual Machine.
         */
        private String keymanagerAlgorithm = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM;

        /**
         * The algorithm used by trust manager factory for SSL connections.
         * Default value is the trust manager factory algorithm configured for the Java Virtual Machine.
         */
        private String truststoreAlgorithm = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;

        /**
         * The endpoint identification algorithm to validate server hostname using server certificate.
         */
        private String endpointIdentificationAlgorithm = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;

        /**
         * The SecureRandom PRNG implementation to use for SSL cryptography operations.
         */
        private String secureRandomImplementation;

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public List<String> getCipherSuites() {
            return cipherSuites;
        }

        public void setCipherSuites(List<String> cipherSuites) {
            this.cipherSuites = cipherSuites;
        }

        public List<String> getEnabledProtocols() {
            return enabledProtocols;
        }

        public void setEnabledProtocols(List<String> enabledProtocols) {
            this.enabledProtocols = enabledProtocols;
        }

        public String getKeyStoreType() {
            return keyStoreType;
        }

        public void setKeyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
        }

        public FileSystemResource getKeystoreLocation() {
            return keystoreLocation;
        }

        public void setKeystoreLocation(FileSystemResource keystoreLocation) {
            this.keystoreLocation = keystoreLocation;
        }

        public String getKeystorePassword() {
            return keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
        }

        public String getTrustStoreType() {
            return trustStoreType;
        }

        public void setTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
        }

        public FileSystemResource getTruststoreLocation() {
            return truststoreLocation;
        }

        public void setTruststoreLocation(FileSystemResource truststoreLocation) {
            this.truststoreLocation = truststoreLocation;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }

        public String getKeymanagerAlgorithm() {
            return keymanagerAlgorithm;
        }

        public void setKeymanagerAlgorithm(String keymanagerAlgorithm) {
            this.keymanagerAlgorithm = keymanagerAlgorithm;
        }

        public String getTruststoreAlgorithm() {
            return truststoreAlgorithm;
        }

        public void setTruststoreAlgorithm(String truststoreAlgorithm) {
            this.truststoreAlgorithm = truststoreAlgorithm;
        }

        public String getEndpointIdentificationAlgorithm() {
            return endpointIdentificationAlgorithm;
        }

        public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm) {
            this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
        }

        public String getSecureRandomImplementation() {
            return secureRandomImplementation;
        }

        public void setSecureRandomImplementation(String secureRandomImplementation) {
            this.secureRandomImplementation = secureRandomImplementation;
        }

        public Map<String, String> buildProperties() {
            Map<String, String> properties = new HashMap<>();

            putString(properties, SslConfigs.SSL_PROTOCOL_CONFIG, this.protocol);
            putString(properties, SslConfigs.SSL_PROVIDER_CONFIG, this.provider);
            putList(properties, SslConfigs.SSL_CIPHER_SUITES_CONFIG, this.cipherSuites);
            putList(properties, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, this.enabledProtocols);
            putString(properties, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, this.keyStoreType);
            putString(properties, SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.keyPassword);
            putString(properties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.keystorePassword);
            putString(properties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, resourceToPath(this.keystoreLocation));

            putString(properties, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, this.trustStoreType);
            putString(properties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, resourceToPath(this.truststoreLocation));
            putString(properties, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.truststorePassword);

            putString(properties, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, this.keymanagerAlgorithm);
            putString(properties, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, this.truststoreAlgorithm);
            putString(properties, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, this.endpointIdentificationAlgorithm);
            putString(properties, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, this.secureRandomImplementation);

            return properties;
        }

        private String resourceToPath(Resource resource) {
            if (resource == null) {
                return null;
            }

            try {
                return resource.getFile().getAbsolutePath();
            } catch (IOException ex) {
                throw new IllegalStateException(
                        "Resource '" + resource + "' must be on a file system", ex);
            }
        }

    }

    public static class Sasl {
        /**
         * SASL mechanism used for client connections. This may be any mechanism for which a security provider is available.
         * GSSAPI is the default mechanism.
         */
        private String mechanism = SaslConfigs.DEFAULT_SASL_MECHANISM;

        /**
         * JAAS login context parameters for SASL connections in the format used by JAAS configuration files.
         * JAAS configuration file format is described <a href=\"http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html\">here</a>.
         * The format for the value is: '<code>loginModuleClass controlFlag (optionName=optionValue)*;</code>'. For brokers,
         * the config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example,
         * listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=com.example.ScramLoginModule required;
         */
        private String jaasConfig;

        /**
         * The fully qualified name of a SASL client callback handler class
         * that implements the AuthenticateCallbackHandler interface.
         */
        private String clientCallbackHandlerClass;

        /**
         * The fully qualified name of a SASL login callback handler class
         * that implements the AuthenticateCallbackHandler interface. For brokers, login callback handler config must be prefixed with
         * listener prefix and SASL mechanism name in lower-case. For example,
         * listener.name.sasl_ssl.scram-sha-256.sasl.login.callback.handler.class=com.example.CustomScramLoginCallbackHandler
         */
        private String loginCallbackHandlerClass;

        /**
         * The fully qualified name of a class that implements the Login interface.
         * For brokers, login config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example,
         * listener.name.sasl_ssl.scram-sha-256.sasl.login.class=com.example.CustomScramLogin
         */
        private String loginClass;

        /**
         * The Kerberos principal name that Kafka runs as.
         * This can be defined either in Kafka's JAAS config or in Kafka's config.
         */
        private String kerberosServiceName;

        /**
         * Kerberos kinit command path.
         */
        private String kerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD;

        /**
         * Login thread will sleep until the specified window factor of time from last refresh
         * to ticket's expiry has been reached, at which time it will try to renew the ticket.
         */
        private Double kerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;

        /**
         * Percentage of random jitter added to the renewal time.
         */
        private Double kerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER;

        /**
         * Login thread sleep time between refresh attempts.
         */
        private Long kerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN;


        /**
         * Login refresh thread will sleep until the specified window factor relative to the
         * credential's lifetime has been reached, at which time it will try to refresh the credential.
         * Legal values are between 0.5 (50%) and 1.0 (100%) inclusive; a default value of 0.8 (80%) is used
         * if no value is specified. Currently applies only to OAUTHBEARER.
         */
        private Double loginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR;

        /**
         * The maximum amount of random jitter relative to the credential's lifetime
         * that is added to the login refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) inclusive;
         * a default value of 0.05 (5%) is used if no value is specified. Currently applies only to OAUTHBEARER.
         */
        private Double loginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER;

        /**
         * The desired minimum time for the login refresh thread to wait before refreshing a credential,
         * in seconds. Legal values are between 0 and 900 (15 minutes); a default value of 60 (1 minute) is used if no value is specified.  This value and
         * sasl.login.refresh.buffer.seconds are both ignored if their sum exceeds the remaining lifetime of a credential.
         */
        private Short loginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS;

        /**
         * The amount of buffer time before credential expiration to maintain when refreshing a credential,
         * in seconds. If a refresh would otherwise occur closer to expiration than the number of buffer seconds then the refresh will be moved up to maintain
         * as much of the buffer time as possible. Legal values are between 0 and 3600 (1 hour); a default value of  300 (5 minutes) is used if no value is specified.
         * This value and sasl.login.refresh.min.period.seconds are both ignored if their sum exceeds the remaining lifetime of a credential.
         * Currently applies only to OAUTHBEARER.
         */
        private Short loginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS;

        public String getKerberosServiceName() {
            return kerberosServiceName;
        }

        public void setKerberosServiceName(String kerberosServiceName) {
            this.kerberosServiceName = kerberosServiceName;
        }

        public String getKerberosKinitCmd() {
            return kerberosKinitCmd;
        }

        public void setKerberosKinitCmd(String kerberosKinitCmd) {
            this.kerberosKinitCmd = kerberosKinitCmd;
        }

        public Double getKerberosTicketRenewWindowFactor() {
            return kerberosTicketRenewWindowFactor;
        }

        public void setKerberosTicketRenewWindowFactor(Double kerberosTicketRenewWindowFactor) {
            this.kerberosTicketRenewWindowFactor = kerberosTicketRenewWindowFactor;
        }

        public Double getKerberosTicketRenewJitter() {
            return kerberosTicketRenewJitter;
        }

        public void setKerberosTicketRenewJitter(Double kerberosTicketRenewJitter) {
            this.kerberosTicketRenewJitter = kerberosTicketRenewJitter;
        }

        public Long getKerberosMinTimeBeforeRelogin() {
            return kerberosMinTimeBeforeRelogin;
        }

        public void setKerberosMinTimeBeforeRelogin(Long kerberosMinTimeBeforeRelogin) {
            this.kerberosMinTimeBeforeRelogin = kerberosMinTimeBeforeRelogin;
        }

        public Double getLoginRefreshWindowFactor() {
            return loginRefreshWindowFactor;
        }

        public void setLoginRefreshWindowFactor(Double loginRefreshWindowFactor) {
            this.loginRefreshWindowFactor = loginRefreshWindowFactor;
        }

        public Double getLoginRefreshWindowJitter() {
            return loginRefreshWindowJitter;
        }

        public void setLoginRefreshWindowJitter(Double loginRefreshWindowJitter) {
            this.loginRefreshWindowJitter = loginRefreshWindowJitter;
        }

        public Short getLoginRefreshMinPeriodSeconds() {
            return loginRefreshMinPeriodSeconds;
        }

        public void setLoginRefreshMinPeriodSeconds(Short loginRefreshMinPeriodSeconds) {
            this.loginRefreshMinPeriodSeconds = loginRefreshMinPeriodSeconds;
        }

        public Short getLoginRefreshBufferSeconds() {
            return loginRefreshBufferSeconds;
        }

        public void setLoginRefreshBufferSeconds(Short loginRefreshBufferSeconds) {
            this.loginRefreshBufferSeconds = loginRefreshBufferSeconds;
        }

        public String getMechanism() {
            return mechanism;
        }

        public void setMechanism(String mechanism) {
            this.mechanism = mechanism;
        }

        public String getJaasConfig() {
            return jaasConfig;
        }

        public void setJaasConfig(String jaasConfig) {
            this.jaasConfig = jaasConfig;
        }

        public String getClientCallbackHandlerClass() {
            return clientCallbackHandlerClass;
        }

        public void setClientCallbackHandlerClass(String clientCallbackHandlerClass) {
            this.clientCallbackHandlerClass = clientCallbackHandlerClass;
        }

        public String getLoginCallbackHandlerClass() {
            return loginCallbackHandlerClass;
        }

        public void setLoginCallbackHandlerClass(String loginCallbackHandlerClass) {
            this.loginCallbackHandlerClass = loginCallbackHandlerClass;
        }

        public String getLoginClass() {
            return loginClass;
        }

        public void setLoginClass(String loginClass) {
            this.loginClass = loginClass;
        }

        public Map<String, String> buildProperties() {
            Map<String, String> properties = new HashMap<>();

            putString(properties, SaslConfigs.SASL_KERBEROS_SERVICE_NAME, kerberosServiceName);
            putString(properties, SaslConfigs.SASL_KERBEROS_KINIT_CMD, kerberosKinitCmd);
            putDouble(properties, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, kerberosTicketRenewWindowFactor);
            putDouble(properties, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, kerberosTicketRenewJitter);
            putLong(properties, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, kerberosMinTimeBeforeRelogin);
            putDouble(properties, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, loginRefreshWindowFactor);
            putDouble(properties, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, loginRefreshWindowJitter);
            putShort(properties, SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, loginRefreshMinPeriodSeconds);
            putShort(properties, SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, loginRefreshBufferSeconds);
            putString(properties, SaslConfigs.SASL_MECHANISM, mechanism);
            putString(properties, SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
            putString(properties, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, clientCallbackHandlerClass);
            putString(properties, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallbackHandlerClass);
            putString(properties, SaslConfigs.SASL_LOGIN_CLASS, loginClass);

            return properties;
        }
    }

    private static class Properties extends HashMap<String, String> {

        public java.util.function.Consumer<String> in(String key) {
            return (value) -> put(key, value);
        }

//        public Properties with(Ssl ssl, Map<String, String> properties) {
//            putAll(ssl.buildProperties());
//            putAll(properties);
//            return this;
//        }

    }
}
