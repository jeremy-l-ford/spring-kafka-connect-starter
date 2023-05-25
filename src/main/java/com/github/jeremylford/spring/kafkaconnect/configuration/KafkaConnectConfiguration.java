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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.boot.autoconfigure.jersey.JerseyProperties;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * Spring configuration that sets up Kafka Connect.
 */
@AutoConfigureBefore(JerseyAutoConfiguration.class)
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({JerseyProperties.class})
public class KafkaConnectConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectConfiguration.class);

    @Bean("kafkaClusterId")
    public String kafkaClusterId(WorkerConfig config) {
        String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
        LOGGER.debug("Kafka cluster ID: {}", kafkaClusterId);
        return kafkaClusterId;
    }

    @Bean
    public ConnectorClientConfigOverridePolicy policy(Plugins plugins, WorkerConfig config) {
        return plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);
    }

    @Bean
    public AdvertisedURL advertisedURL(JerseyProperties jerseyProperties, ServerProperties serverProperties, WorkerConfig config) {
        return new AdvertisedURL(jerseyProperties, serverProperties, config);
    }

    @Bean
    @ConditionalOnMissingBean
    public Time time() {
        return Time.SYSTEM;
    }

    @Bean
    @ConditionalOnMissingBean
    public Plugins plugins(WorkerConfig config) {
        LOGGER.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(config.originalsStrings());
        ClassLoader classLoader = plugins.compareAndSwapWithDelegatingLoader();
        System.out.println("Previous Classloader " + classLoader);
        return plugins;
    }


    @Bean
    public Worker worker(
            Time time,
            WorkerConfig config,
            Plugins plugins,
            OffsetBackingStore offsetBackingStore,
            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
            AdvertisedURL advertisedURL
    ) {
        URI advertisedUrl = advertisedURL.get();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();


        return new Worker(workerId, time, plugins, config, offsetBackingStore, connectorClientConfigOverridePolicy);
    }
}
