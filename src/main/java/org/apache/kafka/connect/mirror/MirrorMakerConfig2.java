package org.apache.kafka.connect.mirror;

import java.util.Map;

public class MirrorMakerConfig2 extends MirrorMakerConfig {

    public MirrorMakerConfig2(Map<?, ?> props) {
        super(props);
    }

    @Override
    public Map<String, String> workerConfig(SourceAndTarget sourceAndTarget) {
        return super.workerConfig(sourceAndTarget);
    }

    @Override
    public Map<String, String> connectorBaseConfig(SourceAndTarget sourceAndTarget, Class<?> connectorClass) {
        return super.connectorBaseConfig(sourceAndTarget, connectorClass);
    }
}
