package com.github.jeremylford.spring.kafkaconnect.configuration;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConnectorPropertiesTest {

    @Test
    public void buildProperties() {
        ConnectorProperties connectorProperties = new ConnectorProperties();
        connectorProperties.setConfigurationReloadAction(Herder.ConfigReloadAction.RESTART);
        connectorProperties.setConnectorClass("connectorClass");
        connectorProperties.setErrorsLogEnable(true);
        connectorProperties.setErrorsRetryMaxDelayMs(100);
        connectorProperties.setErrorsLogIncludeMessages(true);
        connectorProperties.setErrorsRetryTimeoutMs(200);
        connectorProperties.setErrorsTolerance(ToleranceType.ALL);
        connectorProperties.setHeaderConverterClass("headerConverter");
        connectorProperties.setKeyConverterClass("keyConverter");
        connectorProperties.setMaxTasks(100);
        connectorProperties.setName("name");
        connectorProperties.setTransforms(ImmutableList.of("t1", "t2"));
//        connectorProperties.setSink();
        connectorProperties.setTransformDefinitions(
                ImmutableList.of()
        );

        Map<String, String> map = connectorProperties.buildProperties();

        assertEquals(Herder.ConfigReloadAction.RESTART.name().toLowerCase(Locale.ENGLISH),
                map.get(ConnectorConfig.CONFIG_RELOAD_ACTION_CONFIG));
        assertEquals("connectorClass", map.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
        assertEquals("true", map.get(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG));
        assertEquals("100", map.get(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG));
        assertEquals("true", map.get(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG));
        assertEquals("200", map.get(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG));
        assertEquals(ToleranceType.ALL.value(), map.get(ConnectorConfig.ERRORS_TOLERANCE_CONFIG));
        assertEquals("headerConverter", map.get(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG));
        assertEquals("keyConverter", map.get(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG));
        assertEquals("100", map.get(ConnectorConfig.TASKS_MAX_CONFIG));
        assertEquals("name", map.get(ConnectorConfig.NAME_CONFIG));
        assertEquals("t1,t2", map.get(ConnectorConfig.TRANSFORMS_CONFIG));
    }

    @Test
    public void sinkProperties() {

    }

    @Test
    public void defaults() {
        ConnectorProperties connectorProperties = new ConnectorProperties();
        Map<String, String> map = connectorProperties.buildProperties();

//        assertEquals(ConnectorConfig., map.get(ConnectorConfig.CONFIG_RELOAD_ACTION_CONFIG));
//        assertEquals("connectorClass", map.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
//        assertEquals("true", map.get(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG));
//        assertEquals("100", map.get(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG));
//        assertEquals("true", map.get(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG));
//        assertEquals("200", map.get(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG));
//        assertEquals("tolerance", map.get(ConnectorConfig.ERRORS_TOLERANCE_CONFIG));
        assertEquals(ConnectorConfig.HEADER_CONVERTER_CLASS_DEFAULT, map.get(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG));
        assertEquals(String.valueOf(ConnectorConfig.TASKS_MAX_DEFAULT), map.get(ConnectorConfig.TASKS_MAX_CONFIG));
        assertEquals(String.valueOf(ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT), map.get(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG));
        assertEquals(String.valueOf(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT), map.get(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG));
        assertEquals(ConnectorConfig.ERRORS_TOLERANCE_DEFAULT.value(), map.get(ConnectorConfig.ERRORS_TOLERANCE_CONFIG));
        assertEquals(String.valueOf(ConnectorConfig.ERRORS_LOG_ENABLE_DEFAULT), map.get(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG));
        assertEquals(String.valueOf(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT), map.get(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG));
    }
}
