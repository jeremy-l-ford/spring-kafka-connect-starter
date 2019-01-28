package com.github.jeremylford.spring.kafkaconnect.configuration;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaConnectPropertiesTest {

    @Test
    public void buildProperties() {
        KafkaConnectProperties kafkaConnectProperties = new KafkaConnectProperties();

        kafkaConnectProperties.setAccessControlAllowMethods("allowMethods");
        kafkaConnectProperties.setAccessControlAllowOrigin("allowOrigin");
        kafkaConnectProperties.setBootstrapServers(
                ImmutableList.of("b1", "b2")
        );
        kafkaConnectProperties.setClientDnsLookupConfig("clientDns");
        kafkaConnectProperties.setConnectorClientPolicyClass("policyClass");
        kafkaConnectProperties.setConfigProviders(
                ImmutableList.of("p1", "p2")
        );
        kafkaConnectProperties.setConnectors(
                ImmutableList.of(
                        new ConnectorProperties()
                )
        );

        kafkaConnectProperties.setHeaderConverter("headConv");
        kafkaConnectProperties.setKeyConverter("keyConv");
        kafkaConnectProperties.setListeners(
                ImmutableList.of("l1", "l2")
        );
        kafkaConnectProperties.setMetricsReporterClasses(
                "metricReporterClasses"
        );
        kafkaConnectProperties.setMetricsNumSamples(10L);
        kafkaConnectProperties.setMetricsRecordingLevel("level");
        kafkaConnectProperties.setMetricsSampleWindowMs(20L);

        kafkaConnectProperties.setOffsetCommitIntervalMs(30L);
        kafkaConnectProperties.setOffsetCommitTimeoutMs(40L);


        kafkaConnectProperties.setPluginsPath("pluginsPath");
        kafkaConnectProperties.setRestAdvertisedHostName("hostName");
        kafkaConnectProperties.setRestAdvertisedListener("listener");
        kafkaConnectProperties.setRestAdvertisedPort(1123);
        kafkaConnectProperties.setRestExtensionClasses("restExtClasses");
        kafkaConnectProperties.setTaskShutdownGracefulTimeoutMs(432);
        kafkaConnectProperties.setValueConverter("valueConverter");


        Map<String, String> properties = kafkaConnectProperties.buildProperties();


        assertEquals("allowMethods", properties.get(WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG));
        assertEquals("allowOrigin", properties.get(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG));
        assertEquals("b1,b2", properties.get(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("clientDns", properties.get(WorkerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        assertEquals("policyClass", properties.get(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG));
        assertEquals("p1,p2", properties.get(WorkerConfig.CONFIG_PROVIDERS_CONFIG));
        assertEquals("headConv", properties.get(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG));
        assertEquals("keyConv", properties.get(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG));
        assertEquals("l1,l2", properties.get(WorkerConfig.LISTENERS_CONFIG));
        assertEquals("metricReporterClasses", properties.get(WorkerConfig.METRIC_REPORTER_CLASSES_CONFIG));
        assertEquals("10", properties.get(WorkerConfig.METRICS_NUM_SAMPLES_CONFIG));
        assertEquals("level", properties.get(WorkerConfig.METRICS_RECORDING_LEVEL_CONFIG));
        assertEquals("20", properties.get(WorkerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG));
        assertEquals("30", properties.get(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG));
        assertEquals("40", properties.get(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG));
        assertEquals("pluginsPath", properties.get(WorkerConfig.PLUGIN_PATH_CONFIG));
        assertEquals("hostName", properties.get(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG));
        assertEquals("listener", properties.get(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG));
        assertEquals("1123", properties.get(WorkerConfig.REST_ADVERTISED_PORT_CONFIG));
        assertEquals("restExtClasses", properties.get(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG));
        assertEquals("432", properties.get(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG));
        assertEquals("valueConverter", properties.get(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG));

    }

    @Test
    public void buildProperties_default() {
        KafkaConnectProperties kafkaConnectProperties = new KafkaConnectProperties();

        Map<String, String> properties = kafkaConnectProperties.buildProperties();

        assertEquals(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT, properties.get(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG));
        assertEquals(WorkerConfig.HEADER_CONVERTER_CLASS_DEFAULT, properties.get(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG));
        assertEquals(WorkerConfig.BOOTSTRAP_SERVERS_DEFAULT, properties.get(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG));

        assertEquals(String.valueOf(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT), properties.get(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG));
        assertEquals(String.valueOf(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT), properties.get(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG));
    }
}
