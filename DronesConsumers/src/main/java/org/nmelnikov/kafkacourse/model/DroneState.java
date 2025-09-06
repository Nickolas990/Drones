package org.nmelnikov.kafkacourse.model;

import lombok.Data;

import java.time.Instant;

@Data
public class DroneState {
    private String droneName;
    private String lastPosition; // "lat,lon"
    private String lastTemperature;
    private Instant lastMetricTime;
    private Instant lastPositionTime;
}