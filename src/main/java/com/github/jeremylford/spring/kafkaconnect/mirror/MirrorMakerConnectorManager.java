package com.github.jeremylford.spring.kafkaconnect.mirror;

import com.github.jeremylford.spring.kafkaconnect.configuration.KafkaConnectMirrorMakerProperties;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MirrorMakerConnectorManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MirrorMakerConnectorManager.class);

    private final List<Class<?>> CONNECTOR_CLASSES = Arrays.asList(
            MirrorSourceConnector.class,
            MirrorHeartbeatConnector.class,
            MirrorCheckpointConnector.class
    );

    private final Herder herder;
    private final KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties;
    private final MirrorMakerConfig mirrorMakerConfig;

    public MirrorMakerConnectorManager(Herder herder, KafkaConnectMirrorMakerProperties kafkaConnectMirrorMakerProperties, MirrorMakerConfig mirrorMakerConfig) {
        this.herder = herder;
        this.kafkaConnectMirrorMakerProperties = kafkaConnectMirrorMakerProperties;
        this.mirrorMakerConfig = mirrorMakerConfig;
    }

    @PostConstruct
    public void initialize() {
        for (Class<?> connectorClass : CONNECTOR_CLASSES) {
            SourceAndTarget sourceAndTarget = new SourceAndTarget(kafkaConnectMirrorMakerProperties.getSource().getAlias(), kafkaConnectMirrorMakerProperties.getTarget().getAlias());
            Map<String, String> baseConfig = mirrorMakerConfig.connectorBaseConfig(
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
    }
}
