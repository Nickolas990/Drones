package org.nmelnikov.kafkacourse.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.nmelnikov.kafkacourse.model.DroneMetricsMessage;
import org.nmelnikov.kafkacourse.model.DronePositionMessage;
import org.nmelnikov.kafkacourse.model.DroneDeliveryStatusMessage;
import org.nmelnikov.kafkacourse.generator.DroneMessageGenerator;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class DroneProducerManager {
    private final KafkaProducer<String, String> producer;
    private final List<String> droneNames;
    private final DroneMessageGenerator generator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DroneProducerManager(KafkaProducer<String, String> producer,
                                List<String> droneNames,
                                DroneMessageGenerator generator) {
        this.producer = producer;
        this.droneNames = droneNames;
        this.generator = generator;
    }

    public void start() {
        try (ExecutorService exec = Executors.newCachedThreadPool()) {
            for (String drone : droneNames) {
                exec.submit(() -> sendMetricsLoop(drone));
                exec.submit(() -> sendPositionsLoop(drone));
                exec.submit(() -> sendDeliveryStatusesLoop(drone));
            }
        }
    }

    private void sendMetricsLoop(String drone) {
        try {
            while (true) {
                DroneMetricsMessage msg = generator.generateMetrics(drone);
                String value = objectMapper.writeValueAsString(msg);
                producer.send(new ProducerRecord<>("drone_metrics", null, value));
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            log.error("Error sending metrics message", e);
            throw new IllegalStateException(e);
        }
    }

    private void sendPositionsLoop(String drone) {
        try {
            while (true) {
                DronePositionMessage msg = generator.generatePosition(drone);
                String value = objectMapper.writeValueAsString(msg);
                producer.send(new ProducerRecord<>("drone_positions", drone, value));
                Thread.sleep(800);
            }
        } catch (Exception e) {
            log.error("Error sending position message", e);
            throw new IllegalStateException(e);
        }
    }

    private void sendDeliveryStatusesLoop(String drone) {
        Random rnd = new Random();
        try {
            while (true) {
                // В среднем 1 в минуту: от 20 до 100 секунд
                long sleepMs = 20000 + rnd.nextInt(80000);
                Thread.sleep(sleepMs);
                DroneDeliveryStatusMessage msg = generator.generateStatus(drone);
                String value = objectMapper.writeValueAsString(msg);
                producer.send(new ProducerRecord<>("drone_delivery_statuses", drone, value));
            }
        } catch (Exception e) {
            log.error("Error sending delivery status message", e);
            throw new IllegalStateException(e);
        }
    }
}
