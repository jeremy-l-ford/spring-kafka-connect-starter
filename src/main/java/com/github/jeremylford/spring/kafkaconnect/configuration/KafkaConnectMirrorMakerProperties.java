package com.github.jeremylford.spring.kafkaconnect.configuration;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@ConfigurationProperties("spring.kafka.connect.mirror")
public class KafkaConnectMirrorMakerProperties {


    private ConnectorProperties connectorProperties = new ConnectorProperties();

    private List<String> topics = new ArrayList<>();

    private Source source = new Source();

    private Target target = new Target();

    private Map<String, String> configuration = new HashMap<>();

    public ConnectorProperties getConnectorProperties() {
        return connectorProperties;
    }

    public void setConnectorProperties(ConnectorProperties connectorProperties) {
        this.connectorProperties = connectorProperties;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Target getTarget() {
        return target;
    }

    public void setTarget(Target target) {
        this.target = target;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    //    public SourceAndTarget sourceAndTarget() {
//        return new SourceAndTarget();
//    }

    public Properties properties() {
        /*
        public static final String CLUSTERS_CONFIG = "clusters";
    private static final String CLUSTERS_DOC = "List of cluster aliases.";
    public static final String CONFIG_PROVIDERS_CONFIG = WorkerConfig.CONFIG_PROVIDERS_CONFIG;
    private static final String CONFIG_PROVIDERS_DOC = "Names of ConfigProviders to use.";

    private static final String NAME = "name";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    private static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    private static final String GROUP_ID_CONFIG = "group.id";
    private static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    private static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    private static final String HEADER_CONVERTER_CLASS_CONFIG = "header.converter";
    private static final String BYTE_ARRAY_CONVERTER_CLASS =
        "org.apache.kafka.connect.converters.ByteArrayConverter";

    static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";
    static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
         */
        Properties result = new Properties();

        result.put(MirrorMakerConfig.CLUSTERS_CONFIG, source.alias + "," + target.alias);
        result.put(source.alias + "->" + target.alias + ".enable", "true");
        result.put(source.alias + "->" + target.alias + ".enabled", "true");

        PropertySupport.putList((Map) result, source.alias + "->" + target.alias + ".topics", topics);

        result.putAll(source.buildProperties());
        result.putAll(target.buildProperties());
        result.putAll(configuration);
        result.putAll(connectorProperties.buildProperties());

        return result;
    }

    public static class Source {
        private String alias;
        private List<String> bootstrapServers = Collections.singletonList(DistributedConfig.BOOTSTRAP_SERVERS_DEFAULT);

        private DistributedProperties distributedProperties = new DistributedProperties();
        private KafkaProperties.Consumer consumer = new KafkaProperties.Consumer();
        private KafkaProperties.Admin admin = new KafkaProperties.Admin();

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public List<String> getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public DistributedProperties getDistributedProperties() {
            return distributedProperties;
        }

        public void setDistributedProperties(DistributedProperties distributedProperties) {
            this.distributedProperties = distributedProperties;
        }

        public KafkaProperties.Consumer getConsumer() {
            return consumer;
        }

        public void setConsumer(KafkaProperties.Consumer consumer) {
            this.consumer = consumer;
        }

        public KafkaProperties.Admin getAdmin() {
            return admin;
        }

        public void setAdmin(KafkaProperties.Admin admin) {
            this.admin = admin;
        }

        public Properties buildProperties() {
            Properties result = new Properties();

            PropertySupport.putList((Map) result, alias + ".bootstrap.servers", bootstrapServers);

            distributedProperties.buildProperties().forEach((s, s2) -> result.put(alias + "." + s, s2));
            consumer.buildProperties().forEach((s, s2) -> result.put(alias + ".consumer." + s, s2.toString()));
            admin.buildProperties().forEach((s, s2) -> result.put(alias + ".admin." + s, s2.toString()));

            return result;
        }
    }

    public static class Target {
        private String alias;
        private List<String> bootstrapServers = Collections.singletonList(DistributedConfig.BOOTSTRAP_SERVERS_DEFAULT);

        private DistributedProperties distributedProperties = new DistributedProperties();
        private KafkaProperties.Producer producer = new KafkaProperties.Producer();
        private KafkaProperties.Admin admin = new KafkaProperties.Admin();

        //override Spring defaults to match expected MirrorMaker defaults
        {
            producer.setKeySerializer(ByteArraySerializer.class);
            producer.setValueSerializer(ByteArraySerializer.class);
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public List<String> getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public DistributedProperties getDistributedProperties() {
            return distributedProperties;
        }

        public void setDistributedProperties(DistributedProperties distributedProperties) {
            this.distributedProperties = distributedProperties;
        }

        public KafkaProperties.Producer getProducer() {
            return producer;
        }

        public void setProducer(KafkaProperties.Producer producer) {
            this.producer = producer;
        }

        public KafkaProperties.Admin getAdmin() {
            return admin;
        }

        public void setAdmin(KafkaProperties.Admin admin) {
            this.admin = admin;
        }

        public Properties buildProperties() {
            Properties result = new Properties();

            PropertySupport.putList((Map) result, alias + ".bootstrap.servers", bootstrapServers);
            distributedProperties.buildProperties().forEach((s, s2) -> {
                String value = getValue(s2);
                result.put(alias + "." + s, value);
            });
            producer.buildProperties().forEach((s, s2) -> {
                String value = getValue(s2);
                result.put(alias + ".producer." + s, value);
            });
            admin.buildProperties().forEach((s, s2) -> {
                String value = getValue(s2);
                result.put(alias + ".admin." + s, value);
            });

            return result;
        }

        private String getValue(Object s2) {
            if(s2 instanceof Class) {
                return ((Class<?>) s2).getName();
            }
            return s2.toString();
        }
    }
}
