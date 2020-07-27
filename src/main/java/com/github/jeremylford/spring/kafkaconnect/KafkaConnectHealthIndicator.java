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

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.TaskState;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaConnectHealthIndicator extends AbstractHealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectHealthIndicator.class);

    private final Herder herder;
    private final ConnectClusterState connectClusterState;

    public KafkaConnectHealthIndicator(Herder herder, ConnectClusterState connectClusterState) {
        this.herder = herder;
        this.connectClusterState = connectClusterState;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {

        Collection<String> connectorNames = herder.connectors(); //avoid timeout exception
        Map<String, Map<String, String>> connectorState = new HashMap<>(); //TODO: send back map

        AbstractStatus.State status = null;

        for (String connectorName : connectorNames) {

            ConnectorHealth connectorHealth = connectClusterState.connectorHealth(connectorName);

            //TODO: include details optional
            connectorState.put(connectorName, ImmutableMap.of(
                    "tasks", connectorHealth.tasksState().toString(),
                    "state", connectorHealth.connectorState().toString()
            ));
            if (status != AbstractStatus.State.FAILED) {
                AbstractStatus.State connectorStatus = AbstractStatus.State.valueOf(connectorHealth.connectorState().state());
                if (connectorStatus != AbstractStatus.State.RUNNING) {
                    status = connectorStatus;
                    continue;
                }

                for (TaskState taskState : connectorHealth.tasksState().values()) {
                    AbstractStatus.State taskStatus = AbstractStatus.State.valueOf(taskState.state());
                    if (taskStatus != AbstractStatus.State.RUNNING) {
                        status = taskStatus;
                        break;
                    }
                }
            }

        }

        if (status == null) {
            builder.up();
        } else if (status == AbstractStatus.State.FAILED) {
            builder.down();
        } else {
            builder.unknown();
        }

        builder.withDetail("connectorState", connectorState);
    }
}
