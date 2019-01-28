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
 * distributed under the License is distributed on an "AS ISBASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeremylford.spring.kafkaconnect.configuration;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putBoolean;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putInteger;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putList;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putLong;
import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putString;

/**
 * The properties defining a connector's configuration.
 */
public class ConnectorProperties {

    /**
     * Globally unique name to use for this connector.
     */
    private String name;

    /**
     * Name or alias of the class for this connector. Must be a subclass of org.apache.kafka.connect.connector.Connector.
     * If the connector is org.apache.kafka.connect.file.FileStreamSinkConnector, you can either specify this full name,
     * or use "FileStreamSinkor "FileStreamSinkConnectorto make the configuration a bit shorter
     */
    private String connectorClass;

    /**
     * Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
     * This controls the format of the keys in messages written to or read from Kafka, and since this is
     * independent of connectors it allows any connector to work with any serialization format.
     * Examples of common formats include JSON and Avro.
     */
    private String keyConverterClass;

    /**
     * Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
     * This controls the format of the values in messages written to or read from Kafka, and since this is
     * independent of connectors it allows any connector to work with any serialization format.
     * Examples of common formats include JSON and Avro.
     */
    private String valueConverterClass;

    /**
     * HeaderConverter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
     * This controls the format of the header values in messages written to or read from Kafka, and since this is
     * independent of connectors it allows any connector to work with any serialization format.
     * Examples of common formats include JSON and Avro. By default, the SimpleHeaderConverter is used to serialize
     * header values to strings and deserialize them by inferring the schemas.
     */
    private String headerConverterClass;

    /**
     * Maximum number of tasks to use for this connector.
     */
    private int maxTasks = 1;

    /**
     * Aliases for the transformations to be applied to records.
     */
    private List<String> transforms = new ArrayList<>();

    private List<TransformDefinitionProperties> transformDefinitions = new ArrayList<>();

    /**
     * The action that Connect should take on the connector when changes in external
     * configuration providers result in a change in the connector's configuration properties.
     * A value of 'none' indicates that Connect will do nothing.
     * A value of 'restart' indicates that Connect should restart/reload the connector with the
     * updated configuration properties." +
     * The restart may actually be scheduled in the future if the external configuration provider
     * indicates that a configuration value will expire in the future.
     */
    private Herder.ConfigReloadAction configurationReloadAction = Herder.ConfigReloadAction.NONE;

    /**
     * The maximum duration in milliseconds that a failed operation
     * will be reattempted. The default is 0, which means no retries will be attempted. Use -1 for infinite retries.
     */
    private long errorsRetryTimeoutMs = ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT;

    /**
     * The maximum duration in milliseconds between consecutive retry attempts.
     * Jitter will be added to the delay once this limit is reached to prevent thundering herd issues.
     */
    private int errorsRetryMaxDelayMs = ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT;

    /**
     * Behavior for tolerating errors during connector operation. 'none' is the default value
     * and signals that any error will result in an immediate connector task failure; 'all' changes the behavior
     * to skip over problematic records.
     */
    private ToleranceType errorsTolerance = ConnectorConfig.ERRORS_TOLERANCE_DEFAULT;

    /**
     * If true, write each error and the details of the failed operation and problematic record
     * to the Connect application log. This is 'false' by default, so that only errors that are not tolerated are reported.
     */
    private boolean errorsLogEnable = ConnectorConfig.ERRORS_LOG_ENABLE_DEFAULT;

    /**
     * Whether to the include in the log the Connect record that resulted in
     * a failure. This is 'false' by default, which will prevent record keys, values, and headers from being written to log files,
     * although some information such as topic and partition number will still be logged.
     */
    private boolean errorsLogIncludeMessages = ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT;

    private SinkProperties sink = new SinkProperties();

    private Map<String, String> properties = Maps.newHashMap();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConnectorClass() {
        return connectorClass;
    }

    public void setConnectorClass(String connectorClass) {
        this.connectorClass = connectorClass;
    }

    public String getKeyConverterClass() {
        return keyConverterClass;
    }

    public void setKeyConverterClass(String keyConverterClass) {
        this.keyConverterClass = keyConverterClass;
    }

    public String getValueConverterClass() {
        return valueConverterClass;
    }

    public void setValueConverterClass(String valueConverterClass) {
        this.valueConverterClass = valueConverterClass;
    }

    public String getHeaderConverterClass() {
        return headerConverterClass;
    }

    public void setHeaderConverterClass(String headerConverterClass) {
        this.headerConverterClass = headerConverterClass;
    }

    public int getMaxTasks() {
        return maxTasks;
    }

    public void setMaxTasks(int maxTasks) {
        this.maxTasks = maxTasks;
    }

    public List<String> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<String> transforms) {
        this.transforms = transforms;
    }

    public List<TransformDefinitionProperties> getTransformDefinitions() {
        return transformDefinitions;
    }

    public void setTransformDefinitions(List<TransformDefinitionProperties> transformDefinitions) {
        this.transformDefinitions = transformDefinitions;
    }

    public Herder.ConfigReloadAction getConfigurationReloadAction() {
        return configurationReloadAction;
    }

    public void setConfigurationReloadAction(Herder.ConfigReloadAction configurationReloadAction) {
        this.configurationReloadAction = configurationReloadAction;
    }

    public long getErrorsRetryTimeoutMs() {
        return errorsRetryTimeoutMs;
    }

    public void setErrorsRetryTimeoutMs(long errorsRetryTimeoutMs) {
        this.errorsRetryTimeoutMs = errorsRetryTimeoutMs;
    }

    public int getErrorsRetryMaxDelayMs() {
        return errorsRetryMaxDelayMs;
    }

    public void setErrorsRetryMaxDelayMs(int errorsRetryMaxDelayMs) {
        this.errorsRetryMaxDelayMs = errorsRetryMaxDelayMs;
    }

    public ToleranceType getErrorsTolerance() {
        return errorsTolerance;
    }

    public void setErrorsTolerance(ToleranceType errorsTolerance) {
        this.errorsTolerance = errorsTolerance;
    }

    public boolean isErrorsLogEnable() {
        return errorsLogEnable;
    }

    public void setErrorsLogEnable(boolean errorsLogEnable) {
        this.errorsLogEnable = errorsLogEnable;
    }

    public boolean isErrorsLogIncludeMessages() {
        return errorsLogIncludeMessages;
    }

    public void setErrorsLogIncludeMessages(boolean errorsLogIncludeMessages) {
        this.errorsLogIncludeMessages = errorsLogIncludeMessages;
    }

    public SinkProperties getSink() {
        return sink;
    }

    public void setSink(SinkProperties sink) {
        this.sink = sink;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public static class TransformDefinitionProperties {
        private String name;
        private String transformClass;
        private Map<String, String> properties = new HashMap<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTransformClass() {
            return transformClass;
        }

        public void setTransformClass(String transformClass) {
            this.transformClass = transformClass;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        public Map<String, String> buildProperties() {
            Map<String, String> properties = new HashMap<>();

            String prefix = ConnectorConfig.TRANSFORMS_CONFIG + "." + name;
            properties.put(prefix + ".type", transformClass);

            for (Map.Entry<String, String> entry : this.properties.entrySet()) {
                properties.put(prefix + "." + entry.getKey(), entry.getValue());
            }

            return properties;
        }
    }

    public Map<String, String> buildProperties() {
        Map<String, String> properties = new HashMap<>();

        putString(properties, ConnectorConfig.NAME_CONFIG, name);
        putString(properties, ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);
        putString(properties, ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, keyConverterClass);
        putString(properties, ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, valueConverterClass);
        putString(properties, ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, headerConverterClass);
        putInteger(properties, ConnectorConfig.TASKS_MAX_CONFIG, maxTasks);
        putString(properties, ConnectorConfig.CONFIG_RELOAD_ACTION_CONFIG,
                configurationReloadAction == null ? null : configurationReloadAction.name().toLowerCase(Locale.ENGLISH));
        putBoolean(properties, ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, errorsLogEnable);
        putBoolean(properties, ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, errorsLogIncludeMessages);
        putInteger(properties, ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG, errorsRetryMaxDelayMs);
        putString(properties, ConnectorConfig.ERRORS_TOLERANCE_CONFIG,
                errorsTolerance == null ? null : errorsTolerance.value());
        putLong(properties, ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG, errorsRetryTimeoutMs);
        putList(properties, ConnectorConfig.TRANSFORMS_CONFIG, transforms);

        for (TransformDefinitionProperties transformDefinition : transformDefinitions) {
            properties.putAll(transformDefinition.buildProperties());
        }

        properties.putAll(sink.buildProperties());

        properties.putAll(this.properties);

        return properties;
    }

    public static class SinkProperties {

        /**
         * List of topics to consume, separated by commas.
         */
        private List<String> topics = new ArrayList<>();

        /**
         * Regular expression giving topics to consume.
         * Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>.
         * Only one of topics or topicRegex should be specified.
         */
        private String topicRegex;


        /**
         * The name of the topic to be used as the dead letter queue (DLQ) for messages that
         * result in an error when processed by this sink connector, or its transformations or converters.
         * The topic name is blank by default, which means that no messages are to be recorded in the DLQ.
         */
        private String dlqTopic;

        /**
         * Replication factor used to create the dead letter queue topic when it doesn't already exist.
         */
        private int dlqTopicReplicationFactor = SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DEFAULT;

        /**
         * If true, add headers containing error context to the messages
         * written to the dead letter queue. To avoid clashing with headers from the original record, all error context header
         * keys, all error context header keys will start with <code>__connect.errors.</code>
         */
        private boolean dlqContextHeadersEnable = SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT;

        public List<String> getTopics() {
            return topics;
        }

        public void setTopics(List<String> topics) {
            this.topics = topics;
        }

        public String getTopicRegex() {
            return topicRegex;
        }

        public void setTopicRegex(String topicRegex) {
            this.topicRegex = topicRegex;
        }

        public String getDlqTopic() {
            return dlqTopic;
        }

        public void setDlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
        }

        public int getDlqTopicReplicationFactor() {
            return dlqTopicReplicationFactor;
        }

        public void setDlqTopicReplicationFactor(int dlqTopicReplicationFactor) {
            this.dlqTopicReplicationFactor = dlqTopicReplicationFactor;
        }

        public boolean isDlqContextHeadersEnable() {
            return dlqContextHeadersEnable;
        }

        public void setDlqContextHeadersEnable(boolean dlqContextHeadersEnable) {
            this.dlqContextHeadersEnable = dlqContextHeadersEnable;
        }

        public Map<String, String> buildProperties() {
            Map<String, String> properties = new HashMap<>();

            putList(properties, SinkConnectorConfig.TOPICS_CONFIG, topics);
            putString(properties, SinkConnectorConfig.TOPICS_REGEX_CONFIG, topicRegex);
            putString(properties, SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, dlqTopic);
            putBoolean(properties, SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, dlqContextHeadersEnable);
            putInteger(properties, SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, dlqTopicReplicationFactor);

            return properties;
        }
    }
}
