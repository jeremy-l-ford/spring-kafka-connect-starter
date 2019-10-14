# spring-kafka-connect-starter

## Usage

Import the starter jar into your application.  On startup, the kafka connect starter auto conifgures itself based on the supplied configuration.  Configure your application's connectors, transforms, etc. and deploy.

### Example configuration:

```yaml
spring:
  application:
    name: myapp
  kafka:
    connect:
      bootstrap-servers:
        - localhost:9092
      client-dns-lookup-config: default
      key-converter: org.apache.kafka.connect.json.JsonConverter
      value-converter: org.apache.kafka.connect.json.JsonConverter
      header-converter: org.apache.kafka.connect.storage.SimpleHeaderConverter
      task-shutdown-graceful-timeout-ms: 5000
      offset-commit-interval-ms: 60000
      offset-commit-timeout-ms: 5000
      listeners: HTTP://localhost:8080
      rest-advertised-host-name: localhost
      rest-advertised-port: 8080
      rest-advertised-listener: http://localhost:8080
      access-control-allow-origin: "*"
      access-control-allow-methods: GET
      plugins-path: /kafkaconnect/plugins
#      config-providers: xyz #Classname?
#      rest-extension-classes:
      metrics-sample-window-ms: 100
      metrics-num-samples: 10
      metrics-recording-level: INFO
      metrics-reporter-classes: poiuewr

      connectors:
        - name: connector1
          connector-class: org.apache.kafka.connect.tools.MockSourceConnector
          key-converter-class: org.apache.kafka.connect.storage.StringConverter
          value-converter-class: org.apache.kafka.connect.storage.StringConverter
          header-converter-class: org.apache.kafka.connect.storage.SimpleHeaderConverter
          max-tasks: 1
          transforms:
            - t1
            - t2
          configuration-reload-action: NONE
          errors-retry-timeout-ms: 5000
          errors_retry-max-delay-ms: 50000
          errors-tolerance: none
          errors-log-enable: true
          errors-log-include-messages: true
          transform-definitions:
            - name: t1
              transform-class: org.apache.kafka.connect.transforms.InsertField
              properties:
                static.field: myfield
                static.value: myvalue
            - name: t2
              transform-class: org.apache.kafka.connect.transforms.InsertField
              properties:
                static.field: myfield
                static.value: myvalue

      distributed:
        group-id: ${spring.application.name}
        session-timeout-ms: 30000
        heartbeat-interval-ms: 10000
        rebalance-timeout-ms: 30000
        worker-sync-timeout-ms: 5000
        worker-unsync-backoff-ms: 10000
        offset-storage-topic: ${spring.application.name}_offset
        offset-storage-paritions: 2
        offset-storage-replication-factor: 1
        config-topic: ${spring.application.name}_config
        config-storage-replication-factor: 1
        status-storage-topic: ${spring.application.name}_status
        status-storage-paritions: 3
        status-storage-replication-factor: 1


```

### Configuration Refresh

When you application is refreshed to pickup an updated configuration, the Kafka Connect herder will be updated with the 
latest connector/task configurations.  Kafka Connect should then pause, rebalance, and start the connectors/tasks.

## Plugins

### Bundled
If your plugins do not have any version conflicts, plugins can be bundled with the application.

### Plugin Path
If your plugins needs to be Classloader isolated, you can configure the plugin path and install connector/transforms.  See https://cwiki.apache.org/confluence/display/KAFKA/KIP-146+-+Classloading+Isolation+in+Connect.

## Spring Actuator

This starter supplies a health indicator the checks the connection plus provides details on the installed plugins.

When using Jersey with Spring, the actuator endpoint can end up be served/filtered by the jersey.
See https://github.com/spring-projects/spring-boot/issues/17523 on how to have actuator endpoints served by Spring MVC.  

## REST API

The standard Kafka Connect REST api is available under the configured jersey application path.