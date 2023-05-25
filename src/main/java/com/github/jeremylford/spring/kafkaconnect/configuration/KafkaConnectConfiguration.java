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

import com.github.jeremylford.spring.kafkaconnect.AdvertisedURL;
import com.github.jeremylford.spring.kafkaconnect.HerderWithLifeCycle;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.*;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.*;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.boot.autoconfigure.jersey.JerseyProperties;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring configuration that sets up Kafka Connect.
 */
@AutoConfigureBefore(JerseyAutoConfiguration.class)
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({JerseyProperties.class})
public class KafkaConnectConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectConfiguration.class);

    @Bean
    public AdvertisedURL advertisedURL(JerseyProperties jerseyProperties, ServerProperties serverProperties, WorkerConfig config) {
        return new AdvertisedURL(jerseyProperties, serverProperties, config);
    }

    /**
     * Provide the herder that manages the Kafka Connect connectors and tasks.
     */
    @Bean
    public Herder herder(WorkerConfig config, AdvertisedURL advertisedURL) {
        Time time = Time.SYSTEM;
        LOGGER.info("Kafka Connect worker initializing ...");
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();


        LOGGER.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(config.originalsStrings());
        ClassLoader classLoader = plugins.compareAndSwapWithDelegatingLoader();
        System.out.println("Previous Classloader " + classLoader);


        String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
        LOGGER.debug("Kafka cluster ID: {}", kafkaClusterId);

        URI advertisedUrl = advertisedURL.get();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        SharedTopicAdmin sharedAdmin = null;

        LOGGER.info("Initializing offset backing store");
        OffsetBackingStore offsetBackingStore;
        if (config instanceof DistributedConfig) {
            Map<String, Object> adminProps = new HashMap<>(config.originals());
            ConnectUtils.addMetricsContextProperties(adminProps, config, kafkaClusterId);
            sharedAdmin = new SharedTopicAdmin(adminProps);
            offsetBackingStore = new KafkaOffsetBackingStore(sharedAdmin);
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
            StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter, sharedAdmin);
            statusBackingStore.configure(config);

            LOGGER.info("Initializing config backing store");
            ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                    internalValueConverter,
                    config,
                    configTransformer,
                    sharedAdmin
            );

            DistributedConfig distributedConfig = new DistributedConfig(config.originalsStrings());

            herder = new DistributedHerder(distributedConfig, time, worker,
                    kafkaClusterId, statusBackingStore, configBackingStore,
                    advertisedUrl.toString(), connectorClientConfigOverridePolicy, sharedAdmin);
        } else {
            herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy);
        }

        return new HerderWithLifeCycle(herder);
    }
}
