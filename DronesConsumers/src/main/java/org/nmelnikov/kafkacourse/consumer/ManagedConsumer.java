package org.nmelnikov.kafkacourse.consumer;

import java.util.concurrent.atomic.AtomicReference;


public class ManagedConsumer {
    public final ConsumerSupplier consumerSupplier;
    public final String name;
    private final AtomicReference<AutoCloseable> currentConsumer = new AtomicReference<>();

    public ManagedConsumer(ConsumerSupplier supplier, String name) {
        this.consumerSupplier = supplier;
        this.name = name;
    }

    public void pollLoop(AutoCloseable consumer) {
        currentConsumer.set(consumer);
        switch (consumer) {
            case DroneMetricsConsumer c -> c.pollLoop();
            case DeliveryReportConsumer c -> c.pollLoop();
            default -> throw new IllegalStateException("Unknown consumer: " + consumer.getClass());
        }
    }

    public void shutdown() {
        AutoCloseable consumer = currentConsumer.get();
        if (consumer == null) return;
        switch (consumer) {
            case DroneMetricsConsumer c -> c.shutdown();
            case DeliveryReportConsumer c -> c.shutdown();
            default -> {
                try {
                    consumer.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    @FunctionalInterface
    public interface ConsumerSupplier {
        AutoCloseable get();
    }
}
