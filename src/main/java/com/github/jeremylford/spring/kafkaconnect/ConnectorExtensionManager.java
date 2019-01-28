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
package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.connect.health.ConnectClusterDetails;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.health.ConnectClusterDetailsImpl;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.apache.kafka.connect.runtime.rest.ConnectRestConfigurable;
import org.apache.kafka.connect.runtime.rest.ConnectRestExtensionContextImpl;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConnectorExtensionManager {
    private final Logger LOGGER = LoggerFactory.getLogger(ConnectorExtensionManager.class);

    private final List<ConnectRestExtension> extensions = new ArrayList<>();
    private final Herder herder;
    private final ResourceConfig resourceConfig;
    private final WorkerConfig config;

    @Autowired
    public ConnectorExtensionManager(Herder herder, ResourceConfig resourceConfig, WorkerConfig config) {
        this.herder = herder;
        this.resourceConfig = resourceConfig;
        this.config = config;
    }

    @PostConstruct
    public void onStart() {
        List<ConnectRestExtension> extensions = herder.plugins().newPlugins(
                config.getList(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG),
                config, ConnectRestExtension.class);

        Integer rebalanceTimeoutMs = config.getRebalanceTimeout();

        long herderRequestTimeoutMs = ConnectorsResource.REQUEST_TIMEOUT_MS;

        if (rebalanceTimeoutMs != null) {
            herderRequestTimeoutMs = Math.min(herderRequestTimeoutMs, rebalanceTimeoutMs.longValue());
        }

        ConnectClusterDetails connectClusterDetails = new ConnectClusterDetailsImpl(
                herder.kafkaClusterId()
        );

        ConnectRestExtensionContext connectRestExtensionContext =
                new ConnectRestExtensionContextImpl(
                        new ConnectRestConfigurable(resourceConfig),
                        new ConnectClusterStateImpl(herderRequestTimeoutMs, connectClusterDetails, herder)
                );
        for (ConnectRestExtension connectRestExtension : extensions) {
            connectRestExtension.register(connectRestExtensionContext);
            this.extensions.add(connectRestExtension);
        }
    }

    @PreDestroy
    public void onDestroy() {
        for (ConnectRestExtension extension : extensions) {
            try {
                extension.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close extension {}", extension.getClass().getName(), e);
            }
        }
    }
}
