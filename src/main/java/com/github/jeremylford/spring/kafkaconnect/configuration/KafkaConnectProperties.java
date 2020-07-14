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

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putInteger;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putList;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putLong;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putString;

@ConfigurationProperties("spring.kafka.connect")
public class KafkaConnectProperties {

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka
     * cluster. The client will make use of all servers irrespective of which servers are
     * specified here for bootstrapping&mdash;this list only impacts the initial hosts used
     * to discover the full set of servers. This list should be in the form
     * <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the
     * initial connection to discover the full cluster membership (which may change
     * dynamically), this list need not contain the full set of servers (you may want more.
     */
    private List<String> bootstrapServers = Collections.singletonList(DistributedConfig.BOOTSTRAP_SERVERS_DEFAULT);


    private String clientDnsLookupConfig;

    /**
     * Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
     * This controls the format of the keys in messages written to or read from Kafka, and since this is
     * independent of connectors it allows any connector to work with any serialization format.
     * Examples of common formats include JSON and Avro.
     */
    private String keyConverter;

    /**
     * Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
     * This controls the format of the values in messages written to or read from Kafka, and since this is
     * independent of connectors it allows any connector to work with any serialization format.
     * Examples of common formats include JSON and Avro.
     */
    private String valueConverter;

    /**
     * HeaderConverter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
     * This controls the format of the header values in messages written to or read from Kafka, and since this is
     * independent of connectors it allows any connector to work with any serialization format.
     * Examples of common formats include JSON and Avro. By default, the SimpleHeaderConverter is used to serialize
     * header values to strings and deserialize them by inferring the schemas.
     */
    private String headerConverter = WorkerConfig.HEADER_CONVERTER_CLASS_DEFAULT;

    /**
     * Amount of time to wait for tasks to shutdown gracefully. This is the total amount of time,
     * not per task. All task have shutdown triggered, then they are waited on sequentially.
     */
    private long taskShutdownGracefulTimeoutMs = 5000L;

    /**
     * Interval at which to try committing offsets for tasks.
     */
    private long offsetCommitIntervalMs = 60000L;

    /**
     * Maximum number of milliseconds to wait for records to flush and partition offset data to be
     * committed to offset storage before cancelling the process and restoring the offset
     * data to be committed in a future attempt.
     */
    private long offsetCommitTimeoutMs = 5000L;

    /**
     * List of comma-separated URIs the REST API will listen on. The supported protocols are HTTP and HTTPS.
     * Specify hostname as 0.0.0.0 to bind to all interfaces.
     * Leave hostname empty to bind to default interface.
     * Examples of legal listener lists: HTTP://myhost:8083,HTTPS://myhost:8084
     */
    private List<String> listeners = new ArrayList<>();

    /**
     * If this is set, this is the hostname that will be given out to other workers to connect to.
     */
    private String restAdvertisedHostName;

    /**
     * If this is set, this is the port that will be given out to other workers to connect to.
     */
    private Integer restAdvertisedPort;

    /**
     * Sets the advertised listener (HTTP or HTTPS) which will be given to other workers to use.
     */
    private String restAdvertisedListener;

    /**
     * Value to set the Access-Control-Allow-Origin header to for REST API requests.
     * To enable cross origin access, set this to the domain of the application that should be permitted
     * to access the API, or '*' to allow access from any domain. The default value only allows access
     * from the domain of the REST API.
     */
    private String accessControlAllowOrigin = "";

    /**
     * Sets the methods supported for cross origin requests by setting the Access-Control-Allow-Methods header.
     * The default value of the Access-Control-Allow-Methods header allows cross origin requests for GET, POST and HEAD.
     */
    private String accessControlAllowMethods = "";

    /**
     * List of paths separated by commas (,) that
     * contain plugins (connectors, converters, transformations). The list should consist
     * of top level directories that include any combination of:
     * a) directories immediately containing jars with plugins and their dependencies
     * b) uber-jars with plugins and their dependencies
     * c) directories immediately containing the package directory structure of classes of
     * plugins and their dependencies
     * Note: symlinks will be followed to discover dependencies or plugins.
     * Examples: plugin.path=/usr/local/share/java,/usr/local/share/kafka/plugins,
     * /opt/connectors
     */
    private String pluginsPath; // TODO: make need to remove support for this

    /**
     * Comma-separated names of <code>ConfigProvider</code> classes, loaded and used
     * in the order specified. Implementing the interface
     * <code>ConfigProvider</code> allows you to replace variable references in connector configurations,
     * such as for externalized secrets.
     */
    private List<String> configProviders = new ArrayList<>(); // TODO: still needed in spring config land?  on top of spring config?

    /**
     * Comma-separated names of <code>ConnectRestExtension</code> classes, loaded and called
     * in the order specified. Implementing the interface
     * <code>ConnectRestExtension</code> allows you to inject into Connect's REST API user defined resources like filters.
     * Typically used to add custom capability like logging, security, etc.
     */
    private String restExtensionClasses;

    /**
     * Class name or alias of implementation of <code>ConnectorClientConfigOverridePolicy</code>. Defines what client configurations can be
     * overridden by the connector. The default implementation is `None`. The other possible policies in the framework include `All`
     * and `Principal`.
     */
    private String connectorClientPolicyClass = WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT;

    /**
     * The window of time a metrics sample is computed over.
     */
    private Long metricsSampleWindowMs;

    /**
     * The number of samples maintained to compute metrics.
     */
    private Long metricsNumSamples;

    /**
     * The highest recording level for metrics.
     */
    private String metricsRecordingLevel;

    /**
     * A list of classes to use as metrics reporters. Implementing the
     * <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes that
     * will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.
     */
    private String metricsReporterClasses;

    private List<ConnectorProperties> connectors = new ArrayList<>();

    private DistributedProperties distributed = new DistributedProperties();

    private StandaloneProperties standalone = new StandaloneProperties();

    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getClientDnsLookupConfig() {
        return clientDnsLookupConfig;
    }

    public void setClientDnsLookupConfig(String clientDnsLookupConfig) {
        this.clientDnsLookupConfig = clientDnsLookupConfig;
    }

    public String getKeyConverter() {
        return keyConverter;
    }

    public void setKeyConverter(String keyConverter) {
        this.keyConverter = keyConverter;
    }

    public String getValueConverter() {
        return valueConverter;
    }

    public void setValueConverter(String valueConverter) {
        this.valueConverter = valueConverter;
    }

    public String getHeaderConverter() {
        return headerConverter;
    }

    public void setHeaderConverter(String headerConverter) {
        this.headerConverter = headerConverter;
    }

    public long getTaskShutdownGracefulTimeoutMs() {
        return taskShutdownGracefulTimeoutMs;
    }

    public void setTaskShutdownGracefulTimeoutMs(long taskShutdownGracefulTimeoutMs) {
        this.taskShutdownGracefulTimeoutMs = taskShutdownGracefulTimeoutMs;
    }

    public long getOffsetCommitIntervalMs() {
        return offsetCommitIntervalMs;
    }

    public void setOffsetCommitIntervalMs(long offsetCommitIntervalMs) {
        this.offsetCommitIntervalMs = offsetCommitIntervalMs;
    }

    public long getOffsetCommitTimeoutMs() {
        return offsetCommitTimeoutMs;
    }

    public void setOffsetCommitTimeoutMs(long offsetCommitTimeoutMs) {
        this.offsetCommitTimeoutMs = offsetCommitTimeoutMs;
    }

    public List<String> getListeners() {
        return listeners;
    }

    public void setListeners(List<String> listeners) {
        this.listeners = listeners;
    }

    public String getRestAdvertisedHostName() {
        return restAdvertisedHostName;
    }

    public void setRestAdvertisedHostName(String restAdvertisedHostName) {
        this.restAdvertisedHostName = restAdvertisedHostName;
    }

    public Integer getRestAdvertisedPort() {
        return restAdvertisedPort;
    }

    public void setRestAdvertisedPort(Integer restAdvertisedPort) {
        this.restAdvertisedPort = restAdvertisedPort;
    }

    public String getRestAdvertisedListener() {
        return restAdvertisedListener;
    }

    public void setRestAdvertisedListener(String restAdvertisedListener) {
        this.restAdvertisedListener = restAdvertisedListener;
    }

    public String getConnectorClientPolicyClass() {
        return connectorClientPolicyClass;
    }

    public void setConnectorClientPolicyClass(String connectorClientPolicyClass) {
        this.connectorClientPolicyClass = connectorClientPolicyClass;
    }

    public String getAccessControlAllowOrigin() {
        return accessControlAllowOrigin;
    }

    public void setAccessControlAllowOrigin(String accessControlAllowOrigin) {
        this.accessControlAllowOrigin = accessControlAllowOrigin;
    }

    public String getAccessControlAllowMethods() {
        return accessControlAllowMethods;
    }

    public void setAccessControlAllowMethods(String accessControlAllowMethods) {
        this.accessControlAllowMethods = accessControlAllowMethods;
    }

    public String getPluginsPath() {
        return pluginsPath;
    }

    public void setPluginsPath(String pluginsPath) {
        this.pluginsPath = pluginsPath;
    }

    public List<String> getConfigProviders() {
        return configProviders;
    }

    public void setConfigProviders(List<String> configProviders) {
        this.configProviders = configProviders;
    }

    public String getRestExtensionClasses() {
        return restExtensionClasses;
    }

    public void setRestExtensionClasses(String restExtensionClasses) {
        this.restExtensionClasses = restExtensionClasses;
    }

    public Long getMetricsSampleWindowMs() {
        return metricsSampleWindowMs;
    }

    public void setMetricsSampleWindowMs(Long metricsSampleWindowMs) {
        this.metricsSampleWindowMs = metricsSampleWindowMs;
    }

    public Long getMetricsNumSamples() {
        return metricsNumSamples;
    }

    public void setMetricsNumSamples(Long metricsNumSamples) {
        this.metricsNumSamples = metricsNumSamples;
    }

    public String getMetricsRecordingLevel() {
        return metricsRecordingLevel;
    }

    public void setMetricsRecordingLevel(String metricsRecordingLevel) {
        this.metricsRecordingLevel = metricsRecordingLevel;
    }

    public String getMetricsReporterClasses() {
        return metricsReporterClasses;
    }

    public void setMetricsReporterClasses(String metricsReporterClasses) {
        this.metricsReporterClasses = metricsReporterClasses;
    }

    public List<ConnectorProperties> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<ConnectorProperties> connectors) {
        this.connectors = connectors;
    }

    public DistributedProperties getDistributed() {
        return distributed;
    }

    public void setDistributed(DistributedProperties distributed) {
        this.distributed = distributed;
    }

    public StandaloneProperties getStandalone() {
        return standalone;
    }

    public void setStandalone(StandaloneProperties standalone) {
        this.standalone = standalone;
    }

    public Map<String, String> buildProperties() {
        Map<String, String> properties = new HashMap<>();


        putString(properties, WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers));
        putString(properties, WorkerConfig.CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookupConfig);
        putString(properties, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, keyConverter);
        putString(properties, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, valueConverter);
        putString(properties, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, headerConverter);
        putLong(properties, WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, taskShutdownGracefulTimeoutMs);
        putLong(properties, WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, offsetCommitIntervalMs);
        putLong(properties, WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, offsetCommitTimeoutMs);
        putList(properties, WorkerConfig.LISTENERS_CONFIG, listeners);
        putString(properties, WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG, restAdvertisedHostName);
        putInteger(properties, WorkerConfig.REST_ADVERTISED_PORT_CONFIG, restAdvertisedPort);
        putString(properties, WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, restAdvertisedListener);

        putString(properties, WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, accessControlAllowOrigin);
        putString(properties, WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG, accessControlAllowMethods);
        putString(properties, WorkerConfig.PLUGIN_PATH_CONFIG, pluginsPath);
        putList(properties, WorkerConfig.CONFIG_PROVIDERS_CONFIG, configProviders);
        putString(properties, WorkerConfig.REST_EXTENSION_CLASSES_CONFIG, restExtensionClasses);
        putString(properties, WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, connectorClientPolicyClass);

        putLong(properties, WorkerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindowMs);
        putLong(properties, WorkerConfig.METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples);
        putString(properties, WorkerConfig.METRICS_RECORDING_LEVEL_CONFIG, metricsRecordingLevel);
        putString(properties, WorkerConfig.METRIC_REPORTER_CLASSES_CONFIG, metricsReporterClasses);

        if(distributed.isEnabled()) {
            properties.putAll(distributed.buildProperties());
        } else if(standalone.isEnabled()) {
            properties.putAll(standalone.buildProperties());
        }

        return properties;
    }

    @Override
    public String toString() {
        return "KafkaConnectProperties{" +
                "bootstrapServers=" + bootstrapServers +
                ", clientDnsLookupConfig='" + clientDnsLookupConfig + '\'' +
                ", keyConverter='" + keyConverter + '\'' +
                ", valueConverter='" + valueConverter + '\'' +
                ", headerConverter='" + headerConverter + '\'' +
                ", taskShutdownGracefulTimeoutMs=" + taskShutdownGracefulTimeoutMs +
                ", offsetCommitIntervalMs=" + offsetCommitIntervalMs +
                ", offsetCommitTimeoutMs=" + offsetCommitTimeoutMs +
                ", listeners='" + listeners + '\'' +
                ", restAdvertisedHostName='" + restAdvertisedHostName + '\'' +
                ", restAdvertisedPort=" + restAdvertisedPort +
                ", restAdvertisedListener='" + restAdvertisedListener + '\'' +
                ", accessControlAllowOrigin='" + accessControlAllowOrigin + '\'' +
                ", accessControlAllowMethods='" + accessControlAllowMethods + '\'' +
                ", pluginsPath='" + pluginsPath + '\'' +
                ", configProviders='" + configProviders + '\'' +
                ", restExtensionClasses='" + restExtensionClasses + '\'' +
                ", metricsSampleWindowMs=" + metricsSampleWindowMs +
                ", metricsNumSamples=" + metricsNumSamples +
                ", metricsRecordingLevel='" + metricsRecordingLevel + '\'' +
                ", metricReporterClasses='" + metricsReporterClasses + '\'' +
                ", connectors=" + connectors +
                ", distributed=" + distributed +
                ", standalone=" + standalone +
                '}';
    }


    public static class StandaloneProperties {

        private boolean enabled;

        /**
         * File to store offset data in.
         */
        private String offsetStorageFileName;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getOffsetStorageFileName() {
            return offsetStorageFileName;
        }

        public void setOffsetStorageFileName(String offsetStorageFileName) {
            this.offsetStorageFileName = offsetStorageFileName;
        }

        public Map<String, String> buildProperties() {

            Map<String, String> properties = new HashMap<>();
            putString(properties, StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, offsetStorageFileName);
            return properties;
        }

    }
}
