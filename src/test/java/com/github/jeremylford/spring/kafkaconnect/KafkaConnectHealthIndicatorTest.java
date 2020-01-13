package com.github.jeremylford.spring.kafkaconnect;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.util.Callback;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaConnectHealthIndicatorTest {

    @Test
    public void doHealthCheck_noConnectors() throws Exception {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                new KafkaConnectHealthIndicatorProperties(), herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.UP, h.getStatus());
        assertEquals(ImmutableList.of(), h.getDetails().get("connectors"));
    }

    @Test
    public void doHealthCheck_connector_status() throws Exception {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        when(herder.connectors()).thenReturn(
                ImmutableList.of("conn1")
        );


        doAnswer(invocation -> {
            Callback<Collection<String>> callback = invocation.getArgument(0);
            callback.onCompletion(null, ImmutableList.of("conn1"));
            return null;
        }).when(herder).connectors(any());

        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                new KafkaConnectHealthIndicatorProperties(), herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.UP, h.getStatus());
        assertEquals(ImmutableList.of("conn1"), h.getDetails().get("connectors"));
    }

    @Test
    public void doHealthCheck_connector_error() throws Exception {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        when(herder.connectors()).thenReturn(
                ImmutableList.of("conn1")
        );

        doAnswer(invocation -> {
            Callback<Collection<String>> callback = invocation.getArgument(0);
            callback.onCompletion(new RuntimeException(), null);
            return null;
        }).when(herder).connectors(any());

        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                new KafkaConnectHealthIndicatorProperties(), herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.DOWN, h.getStatus());
    }

    @Test
    public void doHealthCheck_connector_timeout() throws Exception {
        Herder herder = mock(Herder.class);
        ConnectClusterState connectClusterState = mock(ConnectClusterState.class);

        when(herder.connectors()).thenReturn(
                ImmutableList.of("conn1")
        );

        doAnswer(new AnswersWithDelay(1000, invocation -> {
            Callback<Collection<String>> callback = invocation.getArgument(0);
            callback.onCompletion(new RuntimeException(), null);
            return null;
            })).when(herder).connectors(any());

        KafkaConnectHealthIndicatorProperties healthIndicatorProperties = new KafkaConnectHealthIndicatorProperties();
        healthIndicatorProperties.setTimeout(500);
        KafkaConnectHealthIndicator indicator = new KafkaConnectHealthIndicator(
                healthIndicatorProperties, herder,
                connectClusterState);

        Health.Builder builder = new Health.Builder();
        indicator.doHealthCheck(builder);

        Health h = builder.build();
        assertEquals(Status.DOWN, h.getStatus());
    }
}
