package org.nmelnikov.kafkacourse.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaTopicUtils {

    public static int getPartitionCount(String topic, String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            DescribeTopicsResult describe = admin.describeTopics(Collections.singletonList(topic));
            TopicDescription desc = describe.values().get(topic).get();
            return desc.partitions().size();
        } catch (Exception ex) {
            log.error("Не удалось получить число партиций для топика {}", topic, ex);
            throw new IllegalStateException(
                    "Не удалось получить число партиций для топика " + topic + ": " + ex.getMessage(), ex);
        }
    }
}
