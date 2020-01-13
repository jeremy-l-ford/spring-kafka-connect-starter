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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Atomics;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.ConnectorState;
import org.apache.kafka.connect.health.TaskState;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaConnectHealthIndicator extends AbstractHealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectHealthIndicator.class);

    private final KafkaConnectHealthIndicatorProperties healthIndicatorProperties;
    private final Herder herder;
    private final ConnectClusterState connectClusterState;

    public KafkaConnectHealthIndicator(KafkaConnectHealthIndicatorProperties healthIndicatorProperties, Herder herder, ConnectClusterState connectClusterState) {
        this.healthIndicatorProperties = healthIndicatorProperties;
        this.herder = herder;
        this.connectClusterState = connectClusterState;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {

//        CountDownLatch countDownLatch = new CountDownLatch(1);

        Collection<String> connectorNames = herder.connectors(); //avoid timeout exception
        Map<String, Map<String, String>> connectorState = new HashMap<>(); //TODO: send back map

//        connectClusterState.`


        builder.up();

        for (String connectorName : connectorNames) {
//            ConnectorStateInfo stateInfo = herder.connectorStatus(connectorName);
//            AbstractStatus.State status = AbstractStatus.State.valueOf(stateInfo.connector().state());

            ConnectorHealth connectorHealth = connectClusterState.connectorHealth(connectorName);
            AbstractStatus.State status = AbstractStatus.State.valueOf(connectorHealth.connectorState().state());
            if (status == AbstractStatus.State.FAILED) {
                builder.down();
            } else if(status != AbstractStatus.State.RUNNING) {
                builder.unknown();
            }

            connectorState.put(connectorName, ImmutableMap.of(
                    "tasks", connectorHealth.tasksState().toString(),
                    "state", connectorHealth.connectorState().toString()
            ));
        }

        builder.withDetail("connectorState", connectorState);

//        AtomicReference<Throwable> errorReference = Atomics.newReference();
//
//
//        try {
//            boolean countdownComplete = countDownLatch.await(healthIndicatorProperties.getTimeout(), TimeUnit.MILLISECONDS);
//            if (!countdownComplete) {
//                LOGGER.info("Failed to retrieve health data in allotted time");
//            }
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//
//        if (errorReference.get() != null) {
//            builder.down(errorReference.get());
//        } else {
//            Collection<String> value = connectorNames.get();
//            if (value == null) {
//                value = ImmutableList.of();
//            }
//            builder.up().withDetail("connectors", value);
//        }
    }
}
