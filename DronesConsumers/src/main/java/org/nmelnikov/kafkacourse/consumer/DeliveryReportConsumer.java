package org.nmelnikov.kafkacourse.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.nmelnikov.kafkacourse.model.*;
import org.nmelnikov.kafkacourse.repository.DroneStateRepository;

import java.io.FileWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DeliveryReportConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final DroneStateRepository stateRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String instanceId;

    private static final String REPORT_FILE = "delivery_report.csv";
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault());

    public DeliveryReportConsumer(String bootstrapServers, String groupId, DroneStateRepository repo, String instanceId) {
        this.stateRepository = repo;
        this.instanceId = instanceId;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("group.instance.id", instanceId);
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("drone_delivery_statuses", "drone_metrics", "drone_positions"));
    }

    public void pollLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        switch (rec.topic()) {
                            case "drone_metrics" -> handleMetric(rec.value());
                            case "drone_positions" -> handlePosition(rec.value());
                            case "drone_delivery_statuses" -> handleDelivery(rec.value());
                        }
                    } catch (Exception e) {
                        log.error("Error processing record", e);
                        throw new IllegalStateException(e);
                    }
                }
                if (Math.random() < 0.02) {
                    log.error("[{}] Simulated random consumer crash!", instanceId);
                    throw new IllegalStateException("Simulated random consumer crash: " + instanceId);
                }
            } catch (WakeupException e) {
                log.info("Какой чудной был сон про дронов!");
                break;
            } catch (Exception e) {
                log.error("Unexpected error", e);
                throw new IllegalStateException(e);
            }
        }
    }

    private void handleMetric(String value) throws Exception {
        DroneMetricsMessage msg = objectMapper.readValue(value, DroneMetricsMessage.class);
        stateRepository.updateTemperature(msg.getDrone(), String.format("%.2f", msg.getTemperature()));
    }

    private void handlePosition(String value) throws Exception {
        DronePositionMessage msg = objectMapper.readValue(value, DronePositionMessage.class);
        stateRepository.updatePosition(msg.getDrone(), msg.getLat() + "," + msg.getLon());
    }

    private void handleDelivery(String value) throws Exception {
        DroneDeliveryStatusMessage msg = objectMapper.readValue(value, DroneDeliveryStatusMessage.class);
        DroneState state = stateRepository.getDroneState(msg.getDrone());
        String now = TIME_FORMAT.format(Instant.now());
        String reportLine = String.join(";",
                msg.getDrone(),
                msg.getDestination(),
                now,
                state != null && state.getLastPosition() != null ? state.getLastPosition() : "n/a",
                state != null && state.getLastTemperature() != null ? state.getLastTemperature() : "n/a"
        );
        appendToFile(reportLine + "\n");
        log.info("Delivery report: {}", reportLine);
    }

    private void appendToFile(String msg) {
        try (FileWriter fw = new FileWriter(REPORT_FILE, true)) {
            fw.write(msg);
        } catch (Exception ex) {
            log.error("Error writing to file", ex);
        }
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void close() {
        shutdown();
        consumer.close();
    }
}
