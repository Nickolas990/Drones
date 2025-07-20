package org.nmelnikov.kafkacourse.repository;

import org.nmelnikov.kafkacourse.model.DroneState;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class DroneStateRepository {
    private final ConcurrentMap<String, DroneState> stateMap = new ConcurrentHashMap<>();

    public void updateTemperature(String drone, String temperature) {
        stateMap.computeIfAbsent(drone, k -> new DroneState())
                .setLastTemperature(temperature);
        stateMap.get(drone).setLastMetricTime(Instant.now());
    }

    public void updatePosition(String drone, String position) {
        stateMap.computeIfAbsent(drone, k -> new DroneState())
                .setLastPosition(position);
        stateMap.get(drone).setLastPositionTime(Instant.now());
    }

    public DroneState getDroneState(String drone) {
        return stateMap.get(drone);
    }
}
