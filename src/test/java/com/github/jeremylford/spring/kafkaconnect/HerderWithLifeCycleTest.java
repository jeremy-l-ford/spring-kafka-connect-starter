package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.junit.Test;

import java.lang.invoke.MethodHandle;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
    public void isLeader_distributedHerder() throws Throwable {

        MethodHandle isLeaderMethod = mock(MethodHandle.class);

        doReturn(isLeaderMethod).when(isLeaderMethod).asType(any());
        doReturn(Boolean.TRUE).when(isLeaderMethod).invoke(any(Herder.class));
//        when(isLeaderMethod.invoke(any(Herder.class))).thenReturn(Boolean.TRUE);

        DistributedHerder distributedHerder = mock(DistributedHerder.class);

        HerderWithLifeCycle herderWithLifeCycle = new HerderWithLifeCycle(
                distributedHerder
        ) {

            @Override
            MethodHandle findIsLeaderMethod(Herder delegate) {
                return isLeaderMethod;
            }
        };

        assertTrue(herderWithLifeCycle.isLeader());
    }
}
