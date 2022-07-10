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

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderRequest;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.springframework.util.ReflectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HerderWithLifeCycle implements Herder {

    private final Herder delegate;
    private final Method isLeaderMethod;
    private final Method configMethod;
    private final Field configStateField;
    private final Method herderMetrics;

    public HerderWithLifeCycle(Herder delegate) {
        this.delegate = delegate;
        this.isLeaderMethod = findIsLeaderMethod(delegate);
        this.configStateField = findConfigStateField(delegate);
        this.configMethod = findConfigMethod(delegate);
        this.herderMetrics = findHerderMetricsMethod(delegate);
    }


    @PostConstruct
    @Override
    public void start() {
        delegate.start();
    }

    @PreDestroy
    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public void connectors(Callback<Collection<String>> callback) {
        delegate.connectors(callback);
    }

    @Override
    public void connectorInfo(String connName, Callback<ConnectorInfo> callback) {
        delegate.connectorInfo(connName, callback);
    }

    @Override
    public void connectorConfig(String connName, Callback<Map<String, String>> callback) {
        delegate.connectorConfig(connName, callback);
    }

    @Override
    public void putConnectorConfig(String connName, Map<String, String> config, boolean allowReplace, Callback<Created<ConnectorInfo>> callback) {
        delegate.putConnectorConfig(connName, config, allowReplace, callback);
    }

    @Override
    public void deleteConnectorConfig(String connName, Callback<Created<ConnectorInfo>> callback) {
        delegate.deleteConnectorConfig(connName, callback);
    }

    @Override
    public void requestTaskReconfiguration(String connName) {
        delegate.requestTaskReconfiguration(connName);
    }

    @Override
    public void taskConfigs(String connName, Callback<List<TaskInfo>> callback) {
        delegate.taskConfigs(connName, callback);
    }

    @Override
    public void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback, InternalRequestSignature requestSignature) {
        delegate.putTaskConfigs(connName, configs, callback, requestSignature);
    }

    @Override
    public ConnectorStateInfo connectorStatus(String connName) {
        return delegate.connectorStatus(connName);
    }

    @Override
    public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id) {
        return delegate.taskStatus(id);
    }

    @Override
    public void restartTask(ConnectorTaskId id, Callback<Void> cb) {
        delegate.restartTask(id, cb);
    }

    @Override
    public void restartConnector(String connName, Callback<Void> cb) {
        delegate.restartConnector(connName, cb);
    }

    @Override
    public HerderRequest restartConnector(long delayMs, String connName, Callback<Void> cb) {
        return delegate.restartConnector(delayMs, connName, cb);
    }

    @Override
    public void pauseConnector(String connector) {
        delegate.pauseConnector(connector);
    }

    @Override
    public void resumeConnector(String connector) {
        delegate.resumeConnector(connector);
    }

    @Override
    public Plugins plugins() {
        return delegate.plugins();
    }

    @Override
    public String kafkaClusterId() {
        return delegate.kafkaClusterId();
    }

    @Override
    public Collection<String> connectors() {
        return delegate.connectors();
    }

    @Override
    public ConnectorInfo connectorInfo(String connName) {
        return delegate.connectorInfo(connName);
    }

    public boolean isDistributed() {
        return delegate instanceof DistributedHerder;
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public ActiveTopicsInfo connectorActiveTopics(String connName) {
        return delegate.connectorActiveTopics(connName);
    }

    @Override
    public void resetConnectorActiveTopics(String connName) {
        delegate.resetConnectorActiveTopics(connName);
    }

    @Override
    public StatusBackingStore statusBackingStore() {
        return delegate.statusBackingStore();
    }

    @Override
    public void validateConnectorConfig(Map<String, String> connectorConfig, Callback<ConfigInfos> callback) {
        delegate.validateConnectorConfig(connectorConfig, callback);
    }

    @Override
    public void validateConnectorConfig(Map<String, String> connectorConfig, Callback<ConfigInfos> callback, boolean doLog) {
        delegate.validateConnectorConfig(connectorConfig, callback, doLog);
    }

    /**
     * This method is intended to be called from within the Herder's queue.
     *
     * @return true is the node is the leader
     */
    public boolean isLeader() {
        return isLeader(delegate);
    }

    @Override
    public void tasksConfig(String connName, Callback<Map<ConnectorTaskId, Map<String, String>>> callback) {
        delegate.tasksConfig(connName, callback);
    }

    private boolean isLeader(Herder herder) {
        boolean result = true;
        if (herder instanceof DistributedHerder) {
            if (isLeaderMethod != null) {
                result = (boolean) ReflectionUtils.invokeMethod(isLeaderMethod, herder);
            }
        }
        return result;
    }

    @VisibleForTesting
    Method findConfigMethod(Herder delegate) {
        Method configMethod = ReflectionUtils.findMethod(delegate.getClass(), "config");
        if (configMethod != null) {
            configMethod.setAccessible(true);
        }
        return configMethod;
    }

    @VisibleForTesting
    Field findConfigStateField(Herder delegate) {
        Field configStateField;
        configStateField = ReflectionUtils.findField(delegate.getClass(), "configState");
        if (configStateField != null) {
            configStateField.setAccessible(true);
        }
        return configStateField;
    }

    @VisibleForTesting
    Method findIsLeaderMethod(Herder delegate) {
        Method isLeaderMethod = ReflectionUtils.findMethod(delegate.getClass(), "isLeader");
        if (isLeaderMethod != null) {
            isLeaderMethod.setAccessible(true);
        }

        return isLeaderMethod;
    }

    Method findHerderMetricsMethod(Herder delegate) {
        if (delegate instanceof DistributedHerder) {
            Method herderMetricsMethod = ReflectionUtils.findMethod(delegate.getClass(), "herderMetrics");
            if (herderMetricsMethod != null) {
                herderMetricsMethod.setAccessible(true);
            }
            return herderMetricsMethod;
        } else {
            return null;
        }
    }
}
