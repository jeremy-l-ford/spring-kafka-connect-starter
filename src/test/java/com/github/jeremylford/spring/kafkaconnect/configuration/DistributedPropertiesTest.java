package com.github.jeremylford.spring.kafkaconnect.configuration;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.junit.Test;
import org.springframework.core.io.FileSystemResource;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributedPropertiesTest {

    @Test
    public void buildProperties() {
        DistributedProperties distributedProperties = new DistributedProperties();

        distributedProperties.setConfigStorageReplicationFactor(10);
        distributedProperties.setConfigTopic("configTopic");
        distributedProperties.setGroupId("groupId");
        distributedProperties.setHeartbeatIntervalMs(100L);
        distributedProperties.setMetadataMaxAge(200L);
        distributedProperties.setOffsetStoragePartitions(20);
        distributedProperties.setOffsetStorageReplicationFactor(30);
        distributedProperties.setOffsetStorageTopic("offsetTopic");
        distributedProperties.setRebalanceTimeoutMs(400L);
        distributedProperties.setSessionTimeoutMs(500L);

        distributedProperties.setStatusStoragePartitions(40);
        distributedProperties.setStatusStorageReplicationFactor(50);
        distributedProperties.setStatusStorageTopic("statusTopic");

        distributedProperties.setWorkerSyncTimeoutMs(600L);
        distributedProperties.setWorkerUnsyncBackoffMs(700L);
        distributedProperties.setScheduledRebalanceMaxDelayMs(100);
        distributedProperties.setConnectProtocol(ConnectProtocolCompatibility.EAGER);

        DistributedProperties.Ssl ssl = new DistributedProperties.Ssl();
        ssl.setCipherSuites(ImmutableList.of("c1", "c2"));
        ssl.setEnabledProtocols(ImmutableList.of("p1", "p2"));
        ssl.setEndpointIdentificationAlgorithm("endpoint");
        ssl.setKeymanagerAlgorithm("keyAlg");
        ssl.setKeyPassword("keyPass");
        ssl.setKeystoreLocation(new FileSystemResource("mypath"));
        ssl.setKeystorePassword("keystorePassword");
        ssl.setProtocol("protocol");
        ssl.setProvider("provider");
        ssl.setSecureRandomImplementation("random");
        ssl.setTruststoreAlgorithm("trustAlg");
        ssl.setTruststoreLocation(new FileSystemResource("trustPath"));
        ssl.setTruststorePassword("trustPass");
        ssl.setTrustStoreType("trustType");
        distributedProperties.setSsl(ssl);


        DistributedProperties.Sasl sasl = new DistributedProperties.Sasl();
        sasl.setClientCallbackHandlerClass("clientCallbackClass");
        sasl.setJaasConfig("jassConfig");
        sasl.setKerberosKinitCmd("initcmd");
        sasl.setKerberosMinTimeBeforeRelogin(15L);
        sasl.setKerberosServiceName("kerServiceName");
        sasl.setKerberosTicketRenewJitter(2.5);
        sasl.setKerberosTicketRenewWindowFactor(3.5);
        sasl.setLoginCallbackHandlerClass("loginCallbackClass");
        sasl.setLoginClass("loginClass");
        sasl.setLoginRefreshBufferSeconds((short)11);
        sasl.setLoginRefreshMinPeriodSeconds((short)12);
        sasl.setLoginRefreshWindowFactor(4.5);
        sasl.setLoginRefreshWindowJitter(5.5);
        sasl.setMechanism("mechanism");
        distributedProperties.setSasl(
                sasl
        );

        Map<String, String> properties = distributedProperties.buildProperties();

        assertEquals("10", properties.get(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG));
        assertEquals("configTopic", properties.get(DistributedConfig.CONFIG_TOPIC_CONFIG));
        assertEquals("groupId", properties.get(DistributedConfig.GROUP_ID_CONFIG));
        assertEquals("100", properties.get(DistributedConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals("200", properties.get(CommonClientConfigs.METADATA_MAX_AGE_CONFIG));
        assertEquals("20", properties.get(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG));
        assertEquals("30", properties.get(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG));
        assertEquals("offsetTopic", properties.get(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG));
        assertEquals("400", properties.get(DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG));
        assertEquals("500", properties.get(DistributedConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals("40", properties.get(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG));
        assertEquals("50", properties.get(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG));
        assertEquals("statusTopic", properties.get(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG));
        assertEquals("600", properties.get(DistributedConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG));
        assertEquals("700", properties.get(DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG));
        assertEquals(ConnectProtocolCompatibility.EAGER.protocol(), properties.get(DistributedConfig.CONNECT_PROTOCOL_CONFIG));
        assertEquals("100", properties.get(DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG));

        //sasl
        assertEquals("clientCallbackClass", properties.get(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS));
        assertEquals("jassConfig", properties.get(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals("initcmd", properties.get(SaslConfigs.SASL_KERBEROS_KINIT_CMD));
        assertEquals("15", properties.get(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN));
        assertEquals("kerServiceName", properties.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
        assertEquals("2.5", properties.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER));
        assertEquals("3.5", properties.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR));
        assertEquals("loginCallbackClass", properties.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
        assertEquals("loginClass", properties.get(SaslConfigs.SASL_LOGIN_CLASS));
        assertEquals("11", properties.get(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS));
        assertEquals("12", properties.get(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS));
        assertEquals("4.5", properties.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR));
        assertEquals("5.5", properties.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER));
        assertEquals("mechanism", properties.get(SaslConfigs.SASL_MECHANISM));


        //ssl
        assertEquals("c1,c2", properties.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
        assertEquals("p1,p2", properties.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
        assertEquals("endpoint", properties.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        assertEquals("keyAlg", properties.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
        assertEquals("keyPass", properties.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        assertTrue(properties.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG).endsWith("mypath"));
        assertEquals("keystorePassword", properties.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertEquals("protocol", properties.get(SslConfigs.SSL_PROTOCOL_CONFIG));
        assertEquals("provider", properties.get(SslConfigs.SSL_PROVIDER_CONFIG));
        assertEquals("random", properties.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG));
        assertEquals("trustAlg", properties.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
        assertTrue(properties.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).endsWith("trustPath"));
        assertEquals("trustPass", properties.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals("trustType", properties.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
    }

    @Test
    public void buildProperties_defaults() {
        DistributedProperties distributedProperties = new DistributedProperties();

        Map<String, String> properties = distributedProperties.buildProperties();

        assertEquals(String.valueOf(DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT), properties.get(DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG));
//        assertEquals(DistributedConfig.CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT, properties.get(DistributedConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG));
//        assertEquals(DistributedConfig.HEADER_CONVERTER_CLASS_DEFAULT, properties.get(DistributedConfig.HEADER_CONVERTER_CLASS_CONFIG));
//        assertEquals(DistributedConfig.BOOTSTRAP_SERVERS_DEFAULT, properties.get(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(DistributedConfig.CONNECT_PROTOCOL_DEFAULT, properties.get(DistributedConfig.CONNECT_PROTOCOL_CONFIG));
//        assertEquals(String.valueOf(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT), properties.get(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG));
//        assertEquals(DistributedConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, properties.get(DistributedConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG));
//        assertEquals(DistributedConfig.REST_PORT_DEFAULT, properties.get(DistributedConfig.REST_ADVERTISED_PORT_CONFIG));
        assertEquals(String.valueOf(DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT),
                properties.get(DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG));

        assertEquals(SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, properties.get(SaslConfigs.SASL_KERBEROS_KINIT_CMD));
        assertEquals(SaslConfigs.DEFAULT_SASL_MECHANISM, properties.get(SaslConfigs.SASL_MECHANISM));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN), properties.get(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER), properties.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR), properties.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS), properties.get(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS), properties.get(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR), properties.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR));
        assertEquals(String.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER), properties.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER));

        assertEquals(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, properties.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, properties.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, properties.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, properties.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, properties.get(SslConfigs.SSL_PROTOCOL_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, properties.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, properties.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));

    }
}
