package org.nmelnikov.kafkacourse.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.nmelnikov.kafkacourse.config.KafkaConfig;
import org.nmelnikov.kafkacourse.generator.DroneMessageGenerator;
import org.nmelnikov.kafkacourse.kafka.DroneProducerManager;

import java.util.*;
import java.util.stream.IntStream;

public class DroneProducerService {
    private static final List<String> TOPICS = List.of(
            "drone_metrics", "drone_positions", "drone_delivery_statuses"
    );

    private final DroneProducerManager producerManager;

    public DroneProducerService(KafkaConfig config) {
        // Инициализация топиков
        KafkaTopicManager topicManager = new KafkaTopicManager(TOPICS);
        topicManager.ensureTopics(config.getBootstrapServers());

        // Инициализация дронов
        int count = 3 + new Random().nextInt(6);
        List<String> drones = IntStream.range(0, count)
                .mapToObj(i -> "drone-" + (i + 1))
                .toList();

        // Kafka-producer
        Properties props = new Properties();
        props.put("bootstrap.servers", String.join(",", config.getBootstrapServers()));
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("enable.idempotence", "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        DroneMessageGenerator generator = new DroneMessageGenerator();
        this.producerManager = new DroneProducerManager(producer, drones, generator);
    }

    public void start() {
        producerManager.start();
    }
}
