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

import com.github.jeremylford.spring.kafkaconnect.ContextRefreshedListener;
import com.github.jeremylford.spring.kafkaconnect.HerderWithLifeCycle;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.boot.autoconfigure.jersey.JerseyProperties;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.util.UriBuilder;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * Spring configuration that sets up Kafka Connect.
 */
@AutoConfigureBefore(JerseyAutoConfiguration.class)
@Configuration
@EnableConfigurationProperties({KafkaConnectProperties.class, JerseyProperties.class})
public class KafkaConnectConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectConfiguration.class);

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    private final JerseyProperties jerseyProperties;
    private final ServerProperties serverProperties;

    @Autowired
    public KafkaConnectConfiguration(JerseyProperties jerseyProperties, ServerProperties serverProperties) {
        this.jerseyProperties = jerseyProperties;
        this.serverProperties = serverProperties;
    }

    @Bean
    public WorkerConfig workerConfig(KafkaConnectProperties kafkaConnectProperties) {
        DistributedProperties distributed = kafkaConnectProperties.getDistributed();
        KafkaConnectProperties.StandaloneProperties standalone = kafkaConnectProperties.getStandalone();

        if (distributed.isEnabled()) {
            return new DistributedConfig(kafkaConnectProperties.buildProperties());
        } else if (standalone.isEnabled()) {
            return new StandaloneConfig(kafkaConnectProperties.buildProperties());
        } else {
            return null;
        }
    }

    /**
     * Provide the herder that manages the Kafka Connect connectors and tasks.
     */
    @Bean
    public Herder herder(WorkerConfig config) {
        Time time = Time.SYSTEM;
        LOGGER.info("Kafka Connect worker initializing ...");
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();


        LOGGER.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(config.originalsStrings());
        ClassLoader classLoader = plugins.compareAndSwapWithDelegatingLoader(); //TODO: should i still do this?
        System.out.println("Previous Classloader " + classLoader);


        String kafkaClusterId = "myClusterId"; //ConnectUtils.lookupKafkaClusterId(config);
        LOGGER.debug("Kafka cluster ID: {}", kafkaClusterId);

        URI advertisedUrl = getAdvertisedUrl(config, jerseyProperties, serverProperties);
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        LOGGER.info("Initializing offset backing store");
        OffsetBackingStore offsetBackingStore;
        if (config instanceof DistributedConfig) {
            offsetBackingStore = new KafkaOffsetBackingStore();
        } else {
            offsetBackingStore = new FileOffsetBackingStore();
        }
        offsetBackingStore.configure(config);

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        Worker worker = new Worker(workerId, time, plugins, config, offsetBackingStore, connectorClientConfigOverridePolicy);
        WorkerConfigTransformer configTransformer = worker.configTransformer();


        LOGGER.info("Initializing herder");
        Herder herder;
        if (config instanceof DistributedConfig) {
            LOGGER.info("Initializing status backing store");
            Converter internalValueConverter = worker.getInternalValueConverter();
            StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter);
            statusBackingStore.configure(config);

            LOGGER.info("Initializing config backing store");
            ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                    internalValueConverter,
                    config,
                    configTransformer);

            DistributedConfig distributedConfig = new DistributedConfig(config.originalsStrings());

            herder = new DistributedHerder(distributedConfig, time, worker,
                    kafkaClusterId, statusBackingStore, configBackingStore,
                    advertisedUrl.toString(), connectorClientConfigOverridePolicy);
        } else {
            herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy);
        }

        return new HerderWithLifeCycle(herder);
    }

    @Bean
    public ContextRefreshedListener contextRefreshedListener(Herder herder, KafkaConnectProperties kafkaConnectProperties) {
        return new ContextRefreshedListener(herder, kafkaConnectProperties);
    }

    @VisibleForTesting
    static URI getAdvertisedUrl(WorkerConfig config,
                                JerseyProperties jerseyProperties,
                                ServerProperties serverProperties) {
        String advertisedSecurityProtocol = determineAdvertisedProtocol(config);
        Integer advertisedPort = determineAdvertisedPort(config, serverProperties);
        String advertisedHostName = determineAdvertisedHostName(config, serverProperties);

        UriBuilder uriBuilder = UriComponentsBuilder.newInstance();
        uriBuilder.scheme(advertisedSecurityProtocol);
        uriBuilder = uriBuilder.host(advertisedHostName);

        if (advertisedPort != null) {
            uriBuilder = uriBuilder.port(advertisedPort);
        }

        String path = jerseyProperties.getApplicationPath();
        if (path != null) {
            uriBuilder = uriBuilder.path(path);
        }

        return uriBuilder.build();
    }

    private static Integer determineAdvertisedPort(WorkerConfig config, ServerProperties serverProperties) {
        Integer advertisedPort = config.getInt(WorkerConfig.REST_ADVERTISED_PORT_CONFIG);
        if (advertisedPort == null) {
            advertisedPort = serverProperties.getPort();
            if (advertisedPort == null) {
                advertisedPort = 8080; //standard default of springboot
            }
        }
        return advertisedPort;
    }

    private static String determineAdvertisedHostName(WorkerConfig config, ServerProperties serverProperties) {
        String advertisedHostName = config.getString(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG);
        if (advertisedHostName == null) {
            InetAddress address = serverProperties.getAddress();
            if (address != null) {
                advertisedHostName = address.getHostName();
            } else {
                try {
                    advertisedHostName = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    advertisedHostName = "localhost";
                }
            }
        }
        return advertisedHostName;
    }

    /**
     * Original implementation can be found at
     * {@link org.apache.kafka.connect.runtime.rest.RestServer#determineAdvertisedProtocol}
     */
    private static String determineAdvertisedProtocol(WorkerConfig config) {
        String advertisedSecurityProtocol = config.getString(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG);
        if (advertisedSecurityProtocol == null) {
            String listeners = (String) config.originals().get(WorkerConfig.LISTENERS_CONFIG);

            if (listeners == null) {
                return PROTOCOL_HTTP;
            } else {
                listeners = listeners.toLowerCase(Locale.ENGLISH);
            }

            if (listeners.contains(String.format("%s://", PROTOCOL_HTTP))) {
                return PROTOCOL_HTTP;
            } else if (listeners.contains(String.format("%s://", PROTOCOL_HTTPS))) {
                return PROTOCOL_HTTPS;
            } else {
                return PROTOCOL_HTTP;
            }
        } else {
            return advertisedSecurityProtocol.toLowerCase(Locale.ENGLISH);
        }
    }
}
