package com.github.jeremylford.spring.kafkaconnect;

import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectConfiguration;
import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectMirrorMakerProperties;
import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectProperties;
import com.github.jeremylford.spring.kafkaconnect.mirror.MirrorMakerConnectorManager;
import org.apache.kafka.connect.mirror.MirrorMakerConfig2;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Import({
        KafkaConnectConfiguration.class,
        RESTConfiguration.class,

})
@Configuration(proxyBeanMethods = false)
@AutoConfigureBefore(JerseyAutoConfiguration.class)
public class KafkaConnectAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(value = "spring.kafka.connect.distributed.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectProperties.class)
    public class DistributedConfiguration {

        @Bean
        public WorkerConfig distributedWorkerConfig(KafkaConnectProperties kafkaConnectProperties) {
            return new DistributedConfig(kafkaConnectProperties.buildProperties());
        }

        @Bean
        @ConditionalOnProperty(value = "spring.kafka.connect.autoconfigure", havingValue = "true")
        public ContextRefreshedListener contextRefreshedListener(Herder herder, KafkaConnectProperties kafkaConnectProperties) {
            return new ContextRefreshedListener(herder, kafkaConnectProperties);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(value = "spring.kafka.connect.standalone.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectProperties.class)
    public class StandaloneConfiguration {

        @Bean
        public WorkerConfig standaloneWorkerConfig(KafkaConnectProperties kafkaConnectProperties) {
            return new StandaloneConfig(kafkaConnectProperties.buildProperties());
        }

        @Bean
        @ConditionalOnProperty(value = "spring.kafka.connect.autoconfigure", havingValue = "true")
        public ContextRefreshedListener contextRefreshedListener(Herder herder, KafkaConnectProperties kafkaConnectProperties) {
            return new ContextRefreshedListener(herder, kafkaConnectProperties);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(value = "spring.kafka.connect.mirror.enabled", havingValue = "true")
    @EnableConfigurationProperties({KafkaConnectMirrorMakerProperties.class, KafkaConnectProperties.class})
    public class MirrorMakerConfiguration {

        private final Logger LOGGER = LoggerFactory.getLogger(MirrorMakerConfiguration.class);

        private final KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties;

        @Autowired
        public MirrorMakerConfiguration(KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties) {
            this.kafkaConnectMirrorMakerProperties = kafkaConnectMirrorMakerProperties;
        }

        @Bean
        public MirrorMakerConfig2 mirrorMakerConfig2() {
            return new MirrorMakerConfig2(kafkaConnectMirrorMakerProperties.properties());
        }

        @Bean
        public WorkerConfig mirrorMakerWorkConfig(
                MirrorMakerConfig2 mirrorMakerConfig
        ) {
            return new DistributedConfig(mirrorMakerConfig.workerConfig(new SourceAndTarget(
                    kafkaConnectMirrorMakerProperties.getSource().getAlias(),
                    kafkaConnectMirrorMakerProperties.getTarget().getAlias()))
            );
        }

        @Bean
        public MirrorMakerConnectorManager mirrorMakerConnectorManager(
                Herder herder, KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties, MirrorMakerConfig2 mirrorMakerConfig2
        ) {
            return new MirrorMakerConnectorManager(herder, kafkaConnectMirrorMakerProperties, mirrorMakerConfig2);
        }
    }

}
