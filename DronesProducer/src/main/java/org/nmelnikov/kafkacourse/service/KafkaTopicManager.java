package org.nmelnikov.kafkacourse.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaTopicManager {

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 2;

    private final List<String> requiredTopics;

    public KafkaTopicManager(List<String> requiredTopics) {
        this.requiredTopics = requiredTopics;
    }

    public void ensureTopics(Collection<String> bootstrapServers) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers));

        try (Admin admin = Admin.create(config)) {
            Set<String> existingTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS);

            List<NewTopic> topicsToCreate = new ArrayList<>();
            for (String topic : requiredTopics) {
                if (!existingTopics.contains(topic)) {
                    topicsToCreate.add(new NewTopic(topic, PARTITIONS, REPLICATION_FACTOR));
                }
            }

            if (!topicsToCreate.isEmpty()) {
                CreateTopicsResult createTopicsResult = admin.createTopics(topicsToCreate);
                for (NewTopic topic : topicsToCreate) {
                    createTopics(topic, createTopicsResult);
                }
            } else {
                log.info("Все топики уже существуют. Создание не требуется.");
            }
        } catch (Exception e) {
            log.error("Ошибка при обращении к Kafka: {}", e.getMessage());
            throw new IllegalArgumentException(e);
        }
    }

    private static void createTopics(NewTopic topic, CreateTopicsResult createTopicsResult) throws InterruptedException {
        try {
            createTopicsResult.values().get(topic.name()).get();
            log.info("Топик '{}' успешно создан.", topic.name());
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof TopicExistsException) {
                log.info("Топик '{}' уже существует.", topic.name());
            } else {
                log.error("Ошибка при создании топика '{}': {}", topic.name(), ee.getCause().getMessage());
            }
        }
    }
}
