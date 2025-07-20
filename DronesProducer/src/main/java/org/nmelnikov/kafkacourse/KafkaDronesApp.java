package org.nmelnikov.kafkacourse;

import org.nmelnikov.kafkacourse.config.KafkaConfig;
import org.nmelnikov.kafkacourse.service.DroneProducerService;

public class KafkaDronesApp {
    public static void main(String[] args) {
        KafkaConfig config = KafkaConfig.fromArgs(args);
        DroneProducerService producerService = new DroneProducerService(config);
        producerService.start();
    }
}
