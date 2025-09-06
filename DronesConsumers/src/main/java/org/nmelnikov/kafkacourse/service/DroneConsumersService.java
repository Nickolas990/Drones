package org.nmelnikov.kafkacourse.service;

import lombok.extern.slf4j.Slf4j;
import org.nmelnikov.kafkacourse.consumer.DeliveryReportConsumer;
import org.nmelnikov.kafkacourse.consumer.DroneMetricsConsumer;
import org.nmelnikov.kafkacourse.consumer.ManagedConsumer;
import org.nmelnikov.kafkacourse.repository.DroneStateRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DroneConsumersService implements AutoCloseable {

    private final List<ManagedConsumer> managedConsumers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public DroneConsumersService(String bootstrapServers,
                                 String metricsGroupId,
                                 double tempThreshold,
                                 int droneMetricsPartitions,
                                 String reportGroupId,
                                 int deliveryReportPartitions) {

        DroneStateRepository repo = new DroneStateRepository();

        // Запуск consumer'ов для топика с температурой
        for (int i = 0; i < droneMetricsPartitions; i++) {
            String instanceId = metricsGroupId + "-metrics-instance-" + i;
            ManagedConsumer managed = new ManagedConsumer(
                    () -> new DroneMetricsConsumer(
                            bootstrapServers,
                            metricsGroupId,
                            tempThreshold,
                            repo,
                            instanceId
                    ),
                    "DroneMetricsConsumer-" + i
            );
            managedConsumers.add(managed);
        }

        // Запуск consumer'ов для топика delivery_report
        for (int i = 0; i < deliveryReportPartitions; i++) {
            String instanceId = reportGroupId + "-report-instance-" + i;
            ManagedConsumer managed = new ManagedConsumer(
                    () -> new DeliveryReportConsumer(
                            bootstrapServers,
                            reportGroupId,
                            repo,
                            instanceId
                    ),
                    "DeliveryReportConsumer-" + i
            );
            managedConsumers.add(managed);
        }
    }

    public void start() {
        for (ManagedConsumer mc : managedConsumers) {
            Thread t = new Thread(() -> runWithRestarts(mc), mc.name);
            t.start();
            threads.add(t);
        }
    }

    private void runWithRestarts(ManagedConsumer mc) {
        int restarts = 0;
        while (!shuttingDown.get()) {
            try (AutoCloseable consumer = mc.consumerSupplier.get()) {
                try {
                    mc.pollLoop(consumer);
                    // Завершили нормально (по shutdown)
                    break;
                } catch (Throwable t) {
                    restarts++;
                    log.error("[{}] Consumer crashed (attempt {}): {}", mc.name, restarts, t.getMessage(), t);
                    log.info("[{}] Перезапуск consumer через 2 сек...", mc.name);
                    if (!shuttingDown.get()) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } else break;
                }
            } catch (Exception e) {
                log.error("[{}] Не удалось корректно закрыть consumer: {}", mc.name, e.getMessage(), e);
                break;
            }
        }
        log.info("[{}] Consumer loop завершен.", mc.name);
    }

    public void shutdown() {
        shuttingDown.set(true);
        for (ManagedConsumer mc : managedConsumers) {
            mc.shutdown();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Все consumer потоки завершены.");
    }

    @Override
    public void close() {
        shutdown();
    }
}
