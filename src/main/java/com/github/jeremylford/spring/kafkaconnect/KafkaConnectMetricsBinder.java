package com.github.jeremylford.spring.kafkaconnect;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.MeterBinder;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.ToDoubleFunction;

import static java.util.Collections.emptyList;

/**
 * Metrics Binder for Micrometer.  Inspired by
 * <p>
 * https://github.com/micrometer-metrics/micrometer/blob/master/micrometer-core/src/main/java/io/micrometer/core/instrument/binder/kafka/KafkaConsumerMetrics.java
 */
public class KafkaConnectMetricsBinder implements MeterBinder, AutoCloseable {

    private static final String JMX_DOMAIN = "kafka.connect";
    private static final String METRIC_NAME_PREFIX = "kafka.connect.";

    private final MBeanServer mBeanServer;

    private final Iterable<Tag> tags;

    private final List<Runnable> notificationListenerCleanUpRunnables = new CopyOnWriteArrayList<>();

    public KafkaConnectMetricsBinder() {
        this(emptyList());
    }

    public KafkaConnectMetricsBinder(Iterable<Tag> tags) {
        this(getMBeanServer(), tags);
    }

    public KafkaConnectMetricsBinder(MBeanServer mBeanServer, Iterable<Tag> tags) {
        this.mBeanServer = mBeanServer;
        this.tags = tags;
    }

    private static MBeanServer getMBeanServer() {
        List<MBeanServer> mBeanServers = MBeanServerFactory.findMBeanServer(null);
        if (!mBeanServers.isEmpty()) {
            return mBeanServers.get(0);
        }
        return ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        registerMetricsEventually("connect-coordinator-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            if (tags.stream().anyMatch(t -> t.getKey().equals("topic"))) {
                registerGaugeForObject(registry, o, "assigned-connectors", tags, "The latest lag of the partition", "connectors");
                registerGaugeForObject(registry, o, "assigned-tasks", tags, "The average lag of the partition", "tasks");
                registerGaugeForObject(registry, o, "heartbeat-rate", tags, "", "records");
//                registerTimeGaugeForObject(registry, o, "heartbeat-response-time-max", tags, "", "records");
                registerFunctionCounterForObject(registry, o, "heartbeat-total", tags, "", "records");
                registerGaugeForObject(registry, o, "join-rate", tags, "", "records");
                registerGaugeForObject(registry, o, "join-time-avg", tags, "", "records");
                registerGaugeForObject(registry, o, "join-time-max", tags, "", "records");
                registerFunctionCounterForObject(registry, o, "join-total", tags, "", "records");

                registerGaugeForObject(registry, o, "last-hearteat-seconds-ago", tags, "", "records");
                registerGaugeForObject(registry, o, "sync-rate", tags, "", "records");
                registerGaugeForObject(registry, o, "sync-time-avg", tags, "", "records");
                registerGaugeForObject(registry, o, "sync-time-max", tags, "", "records");
                registerFunctionCounterForObject(registry, o, "sync-total", tags, "", "records");
                // metrics reported just per consumer
            }
        });

        registerMetricsEventually("connect-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition

            registerGaugeForObject(registry, o, "connection-close-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "connection-close-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connection-count", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connection-creation-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "connection-creation-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "failed-authentication-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "failed-authentication-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "failed-reauthentication-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "failed-reauthentication-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "incoming-byte-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "incoming-byte-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "io-ratio", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "io-time-ns-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "io-wait-ratio", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "io-wait-time-ns-avg", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "io-waittime-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "iotime-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "network-io-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "network-io-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "outgoing-byte-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "outgoing-byte-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "reauthentication-latency-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "reauthentication-latency-max", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-rate", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-size-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-size-max", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "request-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "response-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "response-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "select-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "select-total", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "successful-authentication-no-reauth-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "successful-authentication-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "successful-authentication-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "successful-reauthentication-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "successful-reauthentication-total", tags, "The latest lag of the partition", "connections");
        });

        registerMetricsEventually("connect-node-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            registerGaugeForObject(registry, o, "incoming-byte-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "incoming-byte-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "outgoing-byte-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "outgoing-byte-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-latency-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-latency-max", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-rate", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-size-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "request-size-max", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "request-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "response-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "response-total", tags, "The latest lag of the partition", "connections");

        });

        registerMetricsEventually("connector-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
//            registerGaugeForObject(registry, o, "connector-class", tags, "The latest lag of the partition", "connections");
//            registerGaugeForObject(registry, o, "connector-class", tags, "The latest lag of the partition", "connections");
        });

        registerMetricsEventually("connector-task-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            registerGaugeForObject(registry, o, "batch-size-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "batch-size-max", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "offset-commit-avg-time-ms", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "offset-commit-failure-percentage", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "offset-commit-max-time-ms", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "offset-commit-success-percentage", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "pause-ratio", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "running-ratio", tags, "The latest lag of the partition", "connections");
//            registerGaugeForObject(registry, o, "status", tags, "The latest lag of the partition", "connections");

        });

        registerMetricsEventually("source-task-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            registerGaugeForObject(registry, o, "poll-batch-avg-time-ms", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "poll-batch-max-time-ms", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "source-record-active-count", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "source-record-active-count-avg", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "source-record-active-count-max", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "source-record-poll-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "source-record-poll-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "source-record-write-rate", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "source-record-write-total", tags, "The latest lag of the partition", "connections");

        });

        registerMetricsEventually("task-error-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            registerGaugeForObject(registry, o, "deadletterqueue-produce-failures", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "deadletterqueue-produce-requests", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "last-error-timestamp", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "total-errors-logged", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "total-record-errors", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "total-record-failures", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "total-record-skipped", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "total-retries", tags, "The latest lag of the partition", "connections");

        });


        registerMetricsEventually("connect-worker-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            registerGaugeForObject(registry, o, "connector-count", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connector-startup-attempts-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connector-startup-failure-percentage", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connector-startup-failure-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connector-startup-success-percentage", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "connector-startup-success-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "task-count", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "task-startup-attempts-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "task-startup-failure-percentage", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "task-startup-failure-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "task-startup-success-percentage", tags, "The latest lag of the partition", "connections");
            registerFunctionCounterForObject(registry, o, "task-startup-success-total", tags, "The latest lag of the partition", "connections");
        });

        registerMetricsEventually("connect-worker-rebalance-metrics", (o, tags) -> {
            // metrics reported per consumer, topic and partition
            registerFunctionCounterForObject(registry, o, "completed-rebalances-total", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "epoch", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "rebalance-avg-time-ms", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "rebalance-max-time-ms", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "rebalancing", tags, "The latest lag of the partition", "connections");
            registerGaugeForObject(registry, o, "time-since-last-rebalance-ms", tags, "The latest lag of the partition", "connections");

        });
        
    }


    private void registerMetricsEventually(String type, BiConsumer<ObjectName, Tags> perObject) {
        try {
            Set<ObjectName> objs = mBeanServer.queryNames(new ObjectName(JMX_DOMAIN + ":type=" + type + ",*"), null);
            if (!objs.isEmpty()) {
                for (ObjectName o : objs) {
                    perObject.accept(o, Tags.concat(tags, nameTag(o)));
                }
                return;
            }
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Error registering Kafka JMX based metrics", e);
        }

        registerNotificationListener(type, perObject);
    }

    /**
     * This notification listener should remain indefinitely since new Kafka consumers can be added at any time.
     *
     * @param type      The Kafka JMX type to listen for.
     * @param perObject Metric registration handler when a new MBean is created.
     */
    private void registerNotificationListener(String type, BiConsumer<ObjectName, Tags> perObject) {
        NotificationListener notificationListener = (notification, handback) -> {
            MBeanServerNotification mbs = (MBeanServerNotification) notification;
            ObjectName o = mbs.getMBeanName();
            perObject.accept(o, Tags.concat(tags, nameTag(o)));
        };

        NotificationFilter filter = (NotificationFilter) notification -> {
            if (!MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notification.getType()))
                return false;
            ObjectName obj = ((MBeanServerNotification) notification).getMBeanName();
            return obj.getDomain().equals(JMX_DOMAIN) && obj.getKeyProperty("type").equals(type);
        };

        try {
            mBeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, notificationListener, filter, null);
            notificationListenerCleanUpRunnables.add(() -> {
                try {
                    mBeanServer.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, notificationListener);
                } catch (InstanceNotFoundException | ListenerNotFoundException ignored) {
                }
            });
        } catch (InstanceNotFoundException e) {
            throw new RuntimeException("Error registering Kafka MBean listener", e);
        }
    }

    private Iterable<Tag> nameTag(ObjectName name) {
        Tags tags = Tags.empty();

        String clientId = name.getKeyProperty("client-id");
        if (clientId != null) {
            tags = Tags.concat(tags, "client.id", clientId);
        }

        String topic = name.getKeyProperty("topic");
        if (topic != null) {
            tags = Tags.concat(tags, "topic", topic);
        }

        String partition = name.getKeyProperty("partition");
        if (partition != null) {
            tags = Tags.concat(tags, "partition", partition);
        }

        return tags;
    }

    private static String sanitize(String value) {
        return value.replaceAll("-", ".");
    }

    @Override
    public void close() {
        notificationListenerCleanUpRunnables.forEach(Runnable::run);
    }

    private ToDoubleFunction<MBeanServer> getJmxAttribute(MeterRegistry registry, AtomicReference<? extends Meter> meter,
                                                          ObjectName o, String jmxMetricName) {
        return s -> safeDouble(jmxMetricName, () -> {
            if (!s.isRegistered(o)) {
                registry.remove(meter.get());
            }
            return s.getAttribute(o, jmxMetricName);
        });
    }

    private double safeDouble(String jmxMetricName, Callable<Object> callable) {
        try {
            return Double.parseDouble(callable.call().toString());
        } catch (Exception e) {
            return Double.NaN;
        }
    }

    private void registerGaugeForObject(MeterRegistry registry, ObjectName o, String jmxMetricName, String meterName, Tags allTags, String description, String baseUnit) {
        final AtomicReference<Gauge> gauge = new AtomicReference<>();
        gauge.set(Gauge
                .builder(METRIC_NAME_PREFIX + meterName, mBeanServer,
                        getJmxAttribute(registry, gauge, o, jmxMetricName))
                .description(description)
                .baseUnit(baseUnit)
                .tags(allTags)
                .register(registry));
    }

    private void registerGaugeForObject(MeterRegistry registry, ObjectName o, String jmxMetricName, Tags allTags, String description, String baseUnit) {
        registerGaugeForObject(registry, o, jmxMetricName, sanitize(jmxMetricName), allTags, description, baseUnit);
    }

    private void registerFunctionCounterForObject(MeterRegistry registry, ObjectName o, String jmxMetricName, Tags allTags, String description, String baseUnit) {
        final AtomicReference<FunctionCounter> counter = new AtomicReference<>();
        counter.set(FunctionCounter
                .builder(METRIC_NAME_PREFIX + sanitize(jmxMetricName), mBeanServer,
                        getJmxAttribute(registry, counter, o, jmxMetricName))
                .description(description)
                .baseUnit(baseUnit)
                .tags(allTags)
                .register(registry));
    }

    private void registerTimeGaugeForObject(MeterRegistry registry, ObjectName o, String jmxMetricName,
                                            String meterName, Tags allTags, String description, TimeUnit timeUnit) {
        final AtomicReference<TimeGauge> timeGauge = new AtomicReference<>();
        timeGauge.set(TimeGauge.builder(METRIC_NAME_PREFIX + meterName, mBeanServer, timeUnit,
                getJmxAttribute(registry, timeGauge, o, jmxMetricName))
                .description(description)
                .tags(allTags)
                .register(registry));
    }

    private void registerTimeGaugeForObject(MeterRegistry registry, ObjectName o, String jmxMetricName,
                                            String meterName, Tags allTags, String description) {
        registerTimeGaugeForObject(registry, o, jmxMetricName, meterName, allTags, description, TimeUnit.MILLISECONDS);
    }

    private void registerTimeGaugeForObject(MeterRegistry registry, ObjectName o, String jmxMetricName, Tags allTags, String description) {
        registerTimeGaugeForObject(registry, o, jmxMetricName, sanitize(jmxMetricName), allTags, description);
    }
}
