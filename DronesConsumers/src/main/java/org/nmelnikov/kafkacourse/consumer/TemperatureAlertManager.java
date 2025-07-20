package org.nmelnikov.kafkacourse.consumer;

import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

@Slf4j
public class TemperatureAlertManager {

    private final double threshold;
    private final Map<String, List<DataPoint>> droneData = new HashMap<>();
    private final Map<String, Instant> lastAlertTime = new HashMap<>();

    // Поддержка ограниченного количества дронов — память не переполнится
    private static final int MAX_DRONES = 8;
    private static final String ALERT_FILE = "drone_temperature_alerts.log";

    private record DataPoint(Instant ts, double value) {
    }

    public TemperatureAlertManager(double threshold) {
        this.threshold = threshold;
    }

    public void registerMeasurement(String drone, double temperature) {
        if (!droneData.containsKey(drone) && droneData.size() >= MAX_DRONES) return;
        droneData.computeIfAbsent(drone, k -> new ArrayList<>())
                .add(new DataPoint(Instant.now(), temperature));
        checkAndAlert(drone);
    }

    private void checkAndAlert(String drone) {
        // Оставляем только последние 60 секунд
        Instant cutoff = Instant.now().minusSeconds(60);
        List<DataPoint> data = droneData.get(drone);
        data.removeIf(dp -> dp.ts.isBefore(cutoff));
        if (data.isEmpty()) return;

        double avg = data.stream().mapToDouble(dp -> dp.value).average().orElse(0.0);
        Instant lastAlert = lastAlertTime.getOrDefault(drone, Instant.MIN);
        if (avg > threshold && lastAlert.plusSeconds(60).isBefore(Instant.now())) {
            String msg = String.format("ALERT: у дрона %s средняя температура за минуту %.2f°C%n", drone, avg);
            log.info(msg);
            appendToFile(msg);
            lastAlertTime.put(drone, Instant.now());
        }
    }

    private void appendToFile(String msg) {
        try (FileWriter fw = new FileWriter(ALERT_FILE, true)) {
            fw.write(msg);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
