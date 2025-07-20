package org.nmelnikov.kafkacourse;

import lombok.extern.slf4j.Slf4j;
import org.nmelnikov.kafkacourse.config.KafkaConfig;
import org.nmelnikov.kafkacourse.service.DroneConsumersService;
import org.nmelnikov.kafkacourse.util.KafkaTopicUtils;

import java.util.Scanner;

@Slf4j
public class DroneConsumerApp {
    public static void main(String[] args) {

        KafkaConfig config = KafkaConfig.fromArgs(args);
        String kafkaServers = String.join(",", config.getBootstrapServers());
        String metricsGroupId = "drone-metrics-alert-group";
        String reportGroupId = "delivery-report-group";
        double tempThreshold = 40.0;

        int metricsPartitions = KafkaTopicUtils.getPartitionCount("drone_metrics", kafkaServers);
        int reportPartitions = KafkaTopicUtils.getPartitionCount("drone_delivery_statuses", kafkaServers);

        log.info("Используем {} консьюмер(ов) для drone_metrics и {} для drone_delivery_statuses",
                metricsPartitions, reportPartitions);

        try (DroneConsumersService service = new DroneConsumersService(
                kafkaServers,
                metricsGroupId, tempThreshold, metricsPartitions,
                reportGroupId, reportPartitions
        )) {
            service.start();

            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    String cmd = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(cmd.trim())) {
                        log.info("Завершение приложения...");
                        service.shutdown();
                        break;
                    }
                }
            }

            log.info("Приложение завершено.");
        }
    }
}
