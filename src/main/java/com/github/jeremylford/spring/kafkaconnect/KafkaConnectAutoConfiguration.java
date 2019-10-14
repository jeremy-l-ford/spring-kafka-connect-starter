package com.github.jeremylford.spring.kafkaconnect;

import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectConfiguration;
import com.github.jeremylford.spring.kafkaconnect.configuration.MVCConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Import({
        KafkaConnectConfiguration.class,
        RESTConfiguration.class,
        MVCConfiguration.class

})
@Configuration
@AutoConfigureBefore(JerseyAutoConfiguration.class)
public class KafkaConnectAutoConfiguration {


}
