package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.connect.health.ConnectClusterDetails;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.health.ConnectClusterDetailsImpl;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthIndicatorAutoConfiguration;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AutoConfigureAfter(KafkaConnectAutoConfiguration.class)
@ConditionalOnBean(Herder.class)
@AutoConfigureBefore(HealthIndicatorAutoConfiguration.class)
@ConditionalOnEnabledHealthIndicator("kafkaconnect")
@EnableConfigurationProperties(KafkaConnectHealthIndicatorProperties.class)
public class KafkaConnectHealthIndicatorAutoConfiguration {

    private final KafkaConnectHealthIndicatorProperties kafkaConnectHealthIndicatorProperties;

    @Autowired
    public KafkaConnectHealthIndicatorAutoConfiguration(KafkaConnectHealthIndicatorProperties kafkaConnectHealthIndicatorProperties) {
        this.kafkaConnectHealthIndicatorProperties = kafkaConnectHealthIndicatorProperties;
    }

    @Bean
    public ConnectClusterState connectClusterState(Herder herder) {
        long herderRequestTimeoutMs = ConnectorsResource.REQUEST_TIMEOUT_MS;

        ConnectClusterDetails connectClusterDetails = new ConnectClusterDetailsImpl(
                herder.kafkaClusterId()
        );

        return new ConnectClusterStateImpl(herderRequestTimeoutMs, connectClusterDetails, herder);
    }

    @Bean
    public HealthIndicator kafkaConnectHealthIndicator(Herder herder, ConnectClusterState connectClusterState) {
        return new KafkaConnectHealthIndicator(kafkaConnectHealthIndicatorProperties, herder, connectClusterState);
    }
}
