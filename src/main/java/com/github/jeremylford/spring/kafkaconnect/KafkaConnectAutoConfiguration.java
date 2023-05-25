package com.github.jeremylford.spring.kafkaconnect;

import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectConfiguration;
import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectMirrorMakerProperties;
import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectProperties;
import com.github.jeremylford.spring.kafkaconnect.mirror.MirrorMakerConnectorManager;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.*;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jersey.JerseyAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.HashMap;
import java.util.Map;

@Import({
        KafkaConnectConfiguration.class,
        RESTConfiguration.class,

})
@Configuration(proxyBeanMethods = false)
@AutoConfigureBefore(JerseyAutoConfiguration.class)
public class KafkaConnectAutoConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectAutoConfiguration.class);

    static {
        LOGGER.info("Kafka Connect worker initializing ...");
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();
    }


    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(value = "spring.kafka.connect.distributed.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectProperties.class)
    public static class DistributedConfiguration {

        @Bean
        public DistributedConfig distributedWorkerConfig(KafkaConnectProperties kafkaConnectProperties) {
            return new DistributedConfig(kafkaConnectProperties.buildProperties());
        }

        @Bean
        @ConditionalOnProperty(value = "spring.kafka.connect.autoconfigure", havingValue = "true")
        public ContextRefreshedListener contextRefreshedListener(Herder herder, KafkaConnectProperties kafkaConnectProperties) {
            return new ContextRefreshedListener(herder, kafkaConnectProperties);
        }

        @Bean
        public OffsetBackingStore offsetBackingStore(DistributedConfig config, SharedTopicAdmin topicAdminSupplier) {
            LOGGER.info("Initializing offset backing store");
            OffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(topicAdminSupplier);
            offsetBackingStore.configure(config);
            return offsetBackingStore;
        }

        @ConditionalOnMissingBean
        @Bean
        public SharedTopicAdmin topicAdminSupplier(WorkerConfig config, @Qualifier("kafkaClusterId") String kafkaClusterId) {
            Map<String, Object> adminProps = new HashMap<>(config.originals());
            ConnectUtils.addMetricsContextProperties(adminProps, config, kafkaClusterId);
            return new SharedTopicAdmin(adminProps);
        }

        @Bean
        public ConfigBackingStore configBackingStore(WorkerConfig config, Worker worker, SharedTopicAdmin topicAdminSupplier) {
            LOGGER.info("Initializing config backing store");
            return new KafkaConfigBackingStore(
                    worker.getInternalValueConverter(),
                    config,
                    worker.configTransformer(),
                    topicAdminSupplier
            );
        }

        @Bean
        public StatusBackingStore statusBackingStore(
                Time time,
                WorkerConfig config,
                Worker worker,
                SharedTopicAdmin topicAdminSupplier
        ) {
            LOGGER.info("Initializing status backing store");
            StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, worker.getInternalValueConverter(), topicAdminSupplier);
            statusBackingStore.configure(config);
            return statusBackingStore;
        }

        /**
         * Provide the herder that manages the Kafka Connect connectors and tasks.
         */
        @Bean
        public Herder herder(
                DistributedConfig config,
                AdvertisedURL advertisedURL,
                Time time,
                SharedTopicAdmin topicAdminSupplier,
                Worker worker,
                StatusBackingStore statusBackingStore,
                ConfigBackingStore configBackingStore,
                @Qualifier("kafkaClusterId") String kafkaClusterId,
                ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy

        ) {
            LOGGER.info("Initializing herder");

            Herder herder = new DistributedHerder(config, time, worker,
                    kafkaClusterId, statusBackingStore, configBackingStore,
                    advertisedURL.get().toString(), connectorClientConfigOverridePolicy, topicAdminSupplier);

            return new HerderWithLifeCycle(herder);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(value = "spring.kafka.connect.standalone.enabled", havingValue = "true")
    @EnableConfigurationProperties(KafkaConnectProperties.class)
    public static class StandaloneConfiguration {

        @Bean
        public StandaloneConfig standaloneWorkerConfig(KafkaConnectProperties kafkaConnectProperties) {
            return new StandaloneConfig(kafkaConnectProperties.buildProperties());
        }

        @Bean
        @ConditionalOnProperty(value = "spring.kafka.connect.autoconfigure", havingValue = "true")
        public ContextRefreshedListener contextRefreshedListener(Herder herder, KafkaConnectProperties kafkaConnectProperties) {
            return new ContextRefreshedListener(herder, kafkaConnectProperties);
        }

        @Bean
        public Herder herder(
                Worker worker,
                @Qualifier("kafkaClusterId") String kafkaClusterId,
                ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy
        ) {
            LOGGER.info("Initializing herder");
            Herder herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy);
            return new HerderWithLifeCycle(herder);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(value = "spring.kafka.connect.mirror.enabled", havingValue = "true")
    @EnableConfigurationProperties({KafkaConnectMirrorMakerProperties.class, KafkaConnectProperties.class})
    public static class MirrorMakerConfiguration {

        private final KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties;

        @Autowired
        public MirrorMakerConfiguration(KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties) {
            this.kafkaConnectMirrorMakerProperties = kafkaConnectMirrorMakerProperties;
        }

        @Bean
        public MirrorMakerConfig mirrorMakerConfig2() {
            return new MirrorMakerConfig(kafkaConnectMirrorMakerProperties.properties());
        }

        @Bean
        public DistributedConfig mirrorMakerWorkConfig(
                MirrorMakerConfig mirrorMakerConfig
        ) {
            return new DistributedConfig(mirrorMakerConfig.workerConfig(new SourceAndTarget(
                    kafkaConnectMirrorMakerProperties.getSource().getAlias(),
                    kafkaConnectMirrorMakerProperties.getTarget().getAlias()))
            );
        }

        @Bean
        public MirrorMakerConnectorManager mirrorMakerConnectorManager(
                Herder herder, KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties, MirrorMakerConfig mirrorMakerConfig2
        ) {
            return new MirrorMakerConnectorManager(herder, kafkaConnectMirrorMakerProperties, mirrorMakerConfig2);
        }
    }

}
