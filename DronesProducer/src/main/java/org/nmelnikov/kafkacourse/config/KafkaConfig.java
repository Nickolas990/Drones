package org.nmelnikov.kafkacourse.config;

import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
public class KafkaConfig {

    public static final String KAFKA_BOOTSTRAP_SERVERS_ARG_NAME = "--kafka_bootstrap_servers:";
    private final List<String> bootstrapServers;

    public KafkaConfig(List<String> bootstrapServers) {
        this.bootstrapServers = List.copyOf(bootstrapServers);
    }

    public static KafkaConfig fromArgs(String... args) {
        for (String arg : args) {
            if (arg.startsWith(KAFKA_BOOTSTRAP_SERVERS_ARG_NAME)) {
                String serversRaw = arg.substring(KAFKA_BOOTSTRAP_SERVERS_ARG_NAME.length());
                String[] serversArray = serversRaw.split(",");
                List<String> serverList = Arrays.stream(serversArray)
                        .map(String::trim)
                        .filter(server -> !server.isEmpty())
                        .toList();
                if (serverList.isEmpty()) {
                    throw new IllegalArgumentException("Список bootstrap servers пуст.");
                }
                return new KafkaConfig(serverList);
            }
        }
        throw new IllegalArgumentException(
                String.format("Не указан параметр %s<value>", KAFKA_BOOTSTRAP_SERVERS_ARG_NAME)
        );
    }
}
