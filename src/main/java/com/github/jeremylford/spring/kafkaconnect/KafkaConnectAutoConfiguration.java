package com.github.jeremylford.spring.kafkaconnect;

import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectConfiguration;
import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectProperties;
import org.apache.kafka.connect.runtime.Herder;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Import({
        KafkaConnectConfiguration.class,
        RESTConfiguration.class
})
@Configuration
@AutoConfigureBefore(JerseyAutoConfiguration.class)
public class KafkaConnectAutoConfiguration {

    @Bean
    @ConditionalOnProperty(value = "spring.kafka.connect.autoconfigure", havingValue = "true")
    public ContextRefreshedListener contextRefreshedListener(Herder herder, KafkaConnectProperties kafkaConnectProperties) {
        return new ContextRefreshedListener(herder, kafkaConnectProperties);
    }

}
