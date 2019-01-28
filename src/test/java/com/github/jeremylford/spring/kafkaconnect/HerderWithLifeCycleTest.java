package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HerderWithLifeCycleTest {

    @Test
    public void isLeader_standaloneHerder() {
        Worker worker = mock(Worker.class);

        HerderWithLifeCycle herderWithLifeCycle = new HerderWithLifeCycle(
                new StandaloneHerder(worker, "clusterId",
                        new AllConnectorClientConfigOverridePolicy())
        );

        assertTrue(herderWithLifeCycle.isLeader());
    }

    @Test
    public void isLeader_distributedHerder() throws Exception {

        Method isLeaderMethod = mock(Method.class);
        Field clusterState = mock(Field.class);

        when(isLeaderMethod.invoke(any())).thenReturn(true);

        DistributedHerder distributedHerder = mock(DistributedHerder.class);

        HerderWithLifeCycle herderWithLifeCycle = new HerderWithLifeCycle(
                distributedHerder
        ) {
            @Override
            Field findConfigStateField(Herder delegate) {
                return clusterState;
            }

            @Override
            Method findIsLeaderMethod(Herder delegate) {
                return isLeaderMethod;
            }
        };

        assertTrue(herderWithLifeCycle.isLeader());
    }
}
