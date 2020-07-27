package com.github.jeremylford.spring.kafkaconnect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.health.AbstractState;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.ConnectorState;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.health.TaskState;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.util.Callback;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaConnectHealthIndicatorTest {

    @Test
    public void doHealthCheck_noConnectors() {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.UP, h.getStatus());
        assertEquals(ImmutableMap.of(), h.getDetails().get("connectorState"));
    }

    @Test
    public void doHealthCheck_connector_running() {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        when(herder.connectors()).thenReturn(
                ImmutableList.of("conn1")
        );

        when(connectClusterState.connectorHealth("conn1")).thenReturn(new ConnectorHealth(
                "conn1", new ConnectorState("RUNNING", "workerId", "trace"),
                ImmutableMap.of(1, new TaskState(1, "RUNNING", "workerId", "trace")),
                ConnectorType.SOURCE
        ));

        doAnswer(invocation -> {
            Callback<Collection<String>> callback = invocation.getArgument(0);
            callback.onCompletion(null, ImmutableList.of("conn1"));
            return null;
        }).when(herder).connectors(any());

        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.UP, h.getStatus());
        assertNotNull(h.getDetails().get("connectorState"));
    }

    @Test
    public void doHealthCheck_connector_connectorFailed() throws Exception {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        when(herder.connectors()).thenReturn(
                ImmutableList.of("conn1")
        );

        when(connectClusterState.connectorHealth("conn1")).thenReturn(new ConnectorHealth(
                "conn1", new ConnectorState(AbstractStatus.State.FAILED.name(), "workerId", "trace"),
                ImmutableMap.of(1, new TaskState(1, "RUNNING", "workerId", "trace")),
                ConnectorType.SOURCE
        ));

        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.DOWN, h.getStatus());
    }
}
