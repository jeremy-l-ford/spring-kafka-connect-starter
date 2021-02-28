package com.github.jeremylford.spring.kafkaconnect.configuration;

import com.github.jeremylford.spring.kafkaconnect.HerderWithLifeCycle;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig2;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Configuration

public class WorkerConfiguration {

    @Configuration
    @ConditionalOnProperty(value = "spring.kafka.connect.distributed.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectProperties.class)
    public class DistribuedConfiguration {

        @Bean
        public WorkerConfig distributedWorkerConfig(KafkaConnectProperties kafkaConnectProperties) {
            return new DistributedConfig(kafkaConnectProperties.buildProperties());
        }
    }

    @Configuration
    @ConditionalOnProperty(value = "spring.kafka.connect.standalone.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectProperties.class)
    public class StandaloneConfiguration {

        @Bean
        public WorkerConfig distributedWorkerConfig(KafkaConnectProperties kafkaConnectProperties) {
            return new StandaloneConfig(kafkaConnectProperties.buildProperties());
        }
    }

    @Configuration
    @ConditionalOnProperty(value = "spring.kafka.connect.mirror.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectMirrorMakerProperties.class)
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

        private final List<Class<?>> CONNECTOR_CLASSES = Arrays.asList(
                MirrorSourceConnector.class,
                MirrorHeartbeatConnector.class,
                MirrorCheckpointConnector.class
                );

        @Bean
        public KafkaConnectProperties kafkaConnectProperties(
                MirrorMakerConfig2 mirrorMakerConfig2,
                HerderWithLifeCycle herder
        ) {
            //TODO: move this is a class to manage this
            for (Class<?> connectorClass : CONNECTOR_CLASSES) {
                SourceAndTarget sourceAndTarget = new SourceAndTarget(kafkaConnectMirrorMakerProperties.getSource().getAlias(), kafkaConnectMirrorMakerProperties.getTarget().getAlias());
                Map<String, String> baseConfig = mirrorMakerConfig2.connectorBaseConfig(
                        sourceAndTarget,
                        connectorClass
                );

                herder.putConnectorConfig(connectorClass.getSimpleName(), baseConfig, true, new Callback<Herder.Created<ConnectorInfo>>() {
                    @Override
                    public void onCompletion(Throwable e, Herder.Created<ConnectorInfo> result) {
                        if (e instanceof NotLeaderException) {
                            // No way to determine if the connector is a leader or not beforehand.
                            LOGGER.info("Connector {} is a follower. Using existing configuration.", sourceAndTarget);
                        } else {
                            LOGGER.info("Connector {} configured.", sourceAndTarget, e);
                        }
                    }
                });

            }

            KafkaConnectProperties connectProperties = new KafkaConnectProperties();
            return connectProperties;
        }
    }
}
