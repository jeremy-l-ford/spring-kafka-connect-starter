package com.github.jeremylford.spring.kafkaconnect;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfiguration {

//    @Bean
//    public MeterRegistryCustomizer metricRegistryCustomizer(HerderWithLifeCycle herder) {
//
////        Map<String, String> metrics = herder.herderMetrics();
////        System.out.println(metrics);
//
//
//        return new MeterRegistryCustomizer() {
//            @Override
//            public void customize(MeterRegistry registry) {
//
//            }
//        };
//    }

    @Bean
    public KafkaConnectMetricsBinder kafkaConnectMetricsBinder() {
        return new KafkaConnectMetricsBinder();
    }
}
