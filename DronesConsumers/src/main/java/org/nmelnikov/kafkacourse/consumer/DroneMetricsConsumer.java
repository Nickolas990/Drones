package org.nmelnikov.kafkacourse.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.nmelnikov.kafkacourse.model.DroneMetricsMessage;
import org.nmelnikov.kafkacourse.model.DroneState;
import org.nmelnikov.kafkacourse.repository.DroneStateRepository;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DroneMetricsConsumer implements AutoCloseable {

    private final TemperatureAlertManager alertManager;
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final DroneStateRepository repository;
    private final String instanceId;

    public DroneMetricsConsumer(String bootstrapServers, String groupId, double threshold, DroneStateRepository repository, String instanceId) {
        this.repository = repository;
        this.instanceId = instanceId;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("group.instance.id", instanceId);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList("drone_metrics"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("[{}] Rebalance: revoked {}", instanceId, partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.warn("[{}] Rebalance: assigned {}", instanceId, partitions);
            }
        });

        this.alertManager = new TemperatureAlertManager(threshold);
    }

    public void pollLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        DroneMetricsMessage msg = objectMapper.readValue(rec.value(), DroneMetricsMessage.class);
                        repository.updateTemperature(msg.getDrone(), String.format("%.2f", msg.getTemperature()));
                        alertManager.registerMeasurement(msg.getDrone(), msg.getTemperature());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
                if (Math.random() < 0.02) {
                    log.error("[{}] Simulated random consumer crash!", instanceId);
                    throw new IllegalStateException("Simulated random consumer crash: " + instanceId);
                }
            } catch (WakeupException e) {
                log.warn("[{}] Wakeup exception", instanceId);
                break;
            } catch (Exception e) {
                log.error("[{}] Exception occurred", instanceId, e);
                throw new IllegalStateException(e);
            }
        }
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup(); // Немедленно прервать poll
    }

    @Override
    public void close() {
        shutdown();
        consumer.close();
    }
}
