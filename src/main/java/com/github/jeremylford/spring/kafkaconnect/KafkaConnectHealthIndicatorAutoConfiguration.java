package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.connect.runtime.Herder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@ConditionalOnBean(Herder.class)
@AutoConfigureBefore(HealthContributorAutoConfiguration.class)
@ConditionalOnEnabledHealthIndicator("kafkaconnect")
@EnableConfigurationProperties(KafkaConnectHealthIndicatorProperties.class)
public class KafkaConnectHealthIndicatorAutoConfiguration {

    private final KafkaConnectHealthIndicatorProperties kafkaConnectHealthIndicatorProperties;

    @Autowired
    public KafkaConnectHealthIndicatorAutoConfiguration(KafkaConnectHealthIndicatorProperties kafkaConnectHealthIndicatorProperties) {
        this.kafkaConnectHealthIndicatorProperties = kafkaConnectHealthIndicatorProperties;
    }

    @Bean
    public HealthIndicator kafkaConnectHealthIndicator(Herder herder) {
        return new KafkaConnectHealthIndicator(kafkaConnectHealthIndicatorProperties, herder);
    }
}
