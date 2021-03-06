package com.github.jeremylford.spring.kafkaconnect;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MirrorMakerTestingMain {

    public static void main(String[] args) throws Exception {
//        createTopics();
        sendMessage();
        consumeMessage();
//        dumpTopics();
    }

    private static void dumpTopics() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        try(AdminClient adminClient = KafkaAdminClient.create(properties)) {
            System.out.println(adminClient.listTopics().names().get());
        }

        System.out.println();

        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try(AdminClient adminClient = KafkaAdminClient.create(properties)) {
            System.out.println(adminClient.listTopics().names().get());
        }
    }

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        try(AdminClient adminClient = KafkaAdminClient.create(properties)) {
            CreateTopicsResult result = adminClient.createTopics(
                    Arrays.asList(
                            new NewTopic("s1.test1", 1, (short) 1),
                            new NewTopic("s1.test2", 1, (short) 1)
                    )
            );
            result.all().get();
        }


        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try(AdminClient adminClient = KafkaAdminClient.create(properties)) {

            CreateTopicsResult result = adminClient.createTopics(
                    Arrays.asList(
                            new NewTopic("test1", 1, (short) 1),
                            new NewTopic("test2", 1, (short) 1)
                    )
            );
        }
    }

    private static void consumeMessage() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "mainDemo");


        try(KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Arrays.asList("s1.test1", "s1.test2"));
            int tries = 3;
            while (tries-- > 0) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() + " " + record.value() + " " + record.offset());
                }
            }
        }
    }

    private static void sendMessage() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                properties
        )) {

            for (int i=0; i<10; ++i) {
                producer.send(new ProducerRecord<>("test1", "key" + i, "value" + i)).get();
            }
        }
    }
}
