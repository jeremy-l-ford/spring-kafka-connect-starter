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

import com.github.jeremylford.spring.kafkaconnect.configuration.ConnectorProperties;
import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectProperties;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.distributed.RebalanceNeededException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread view:
 * <p>
 * Spring Event Thread
 * -> Cancel in progress Scheduled Future
 * -> Kafka Connect Herder Thread
 * if success, return to Spring Event Thread
 * if rebalance, delay to scheduled thread
 * <p>
 * Scheduled Thread
 * -> Kafka Connect Thread
 * if success, return Spring Event Thread
 */
public class ContextRefreshedListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContextRefreshedListener.class);

    private final ScheduledExecutorService scheduledExecutorService;

    private final Herder herder;

    private final KafkaConnectProperties workerProperties;
    private final AtomicReference<Future<?>> eventFuture = Atomics.newReference();
    private int rebalanceDelayInSeconds = 10;

    @Autowired
    public ContextRefreshedListener(Herder herder, KafkaConnectProperties workerProperties) {
        this.herder = herder;
        this.workerProperties = workerProperties;
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("kafka-connect-context").build());
        executor.setMaximumPoolSize(1);  //only allow one object in the pool at a time
        this.scheduledExecutorService = Executors.unconfigurableScheduledExecutorService(executor);
    }

    @EventListener({ApplicationReadyEvent.class, ContextRefreshedEvent.class})
    public synchronized void handleContextRefresh(ApplicationEvent event) {
        LOGGER.info("Handling event: {}", event);
        Future<?> existingFuture = this.eventFuture.getAndSet(null);
        if (existingFuture != null) {
            existingFuture.cancel(true);
        }

        this.internalOnEvent();
    }

    private synchronized void onScheduledEvent() {
        //clear the future to avoid cancelling ourselves
        try {
            this.internalOnEvent();
        } finally {
            //should never be non-null, but JIC
            Future<?> existingFuture = this.eventFuture.getAndSet(null);
            if (existingFuture != null) {
                // nothing to do, this is the future for the current thread
            }
        }
    }

    private void internalOnEvent() {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //execute a connector call in order to handle the configuration on the Worker's thread
        herder.connectors((error, result) -> {
            try {
                handleEvent(error, result);
            } finally {
                countDownLatch.countDown();
            }
        });

        LOGGER.info("Waiting for connection configuration to be refreshed");
        try {
            // TODO: consider timeout here
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Connection configuration refreshed");
    }

    /**
     * This method executes on the herder's 'EDT'.
     *
     * @param error          a non Throwable if an error occurred.
     * @param connectorNames Collection of connector names.
     */
    private void handleEvent(Throwable error, Collection<String> connectorNames) {
        if (error instanceof RebalanceNeededException) {
            handleRebalance();
        } else if (!isLeader(herder)) {
            LOGGER.info("Not the leader.  Ignoring event.");
        } else if (error == null) {

            Map<String, Map<String, String>> connectorsToAdd = Maps.newHashMap();
            Map<String, Map<String, String>> connectorsToUpdate = Maps.newHashMap();
            List<String> connectorsToRemove = Lists.newArrayList(connectorNames);

            for (ConnectorProperties connector : workerProperties.getConnectors()) {
                connectorsToRemove.remove(connector.getName());

                ConnectorInfo connectorInfo = herder.connectorInfo(connector.getName());
                if (connectorInfo == null) {
                    connectorsToAdd.put(connector.getName(), connector.buildProperties());
                } else {
                    Map<String, String> connectorConfig = connectorInfo.config();

                    if (connectorConfig == null) {
                        connectorsToAdd.put(connector.getName(), connector.buildProperties());
                    } else {
                        Map<String, String> incomingConfig = connector.buildProperties();
                        if (!Maps.difference(incomingConfig, connectorConfig).areEqual()) {
                            connectorsToUpdate.put(connector.getName(), incomingConfig);
                        }
                    }
                }
            }

            for (Map.Entry<String, Map<String, String>> entry : connectorsToAdd.entrySet()) {
                herder.putConnectorConfig(entry.getKey(), entry.getValue(), false, (addError, result) -> {
                    if (addError == null) {

                        LOGGER.info("{} connector {}", result.created() ? "Added" : "Failed to add", entry.getKey());
                    } else {
                        LOGGER.info("Failed to add connector {}", entry.getKey());
                    }
                });
            }

            for (Map.Entry<String, Map<String, String>> entry : connectorsToUpdate.entrySet()) {
                herder.putConnectorConfig(entry.getKey(), entry.getValue(), true, (updateError, result) -> {
                    if (updateError == null) {
                        LOGGER.info("{} connector {}", result.created() ? "Updated" : "Failed to update", entry.getKey());
                    } else {
                        LOGGER.info("Failed to update connector {}", entry.getKey());
                    }
                });
            }

            for (String connectorName : connectorsToRemove) {
                herder.deleteConnectorConfig(connectorName, (removeError, result) -> {
                    if (removeError == null) {
                        LOGGER.info("{} connector {}", result.created() ? "Removed" : "Failed to remove", connectorName);
                    } else {
                        LOGGER.info("Failed to update connector {}", connectorName);
                    }
                });
            }
        }
    }

    private void handleRebalance() {
        LOGGER.info("Rebalance in progress.  Trying again later...");

        this.eventFuture.set(scheduledExecutorService.schedule(ContextRefreshedListener.this::onScheduledEvent, rebalanceDelayInSeconds, TimeUnit.SECONDS));
    }

    private static boolean isLeader(Herder herder) {
        boolean result = true;
        if (herder instanceof HerderWithLifeCycle) {
            result = ((HerderWithLifeCycle) herder).isLeader();
        }
        return result;
    }

}
