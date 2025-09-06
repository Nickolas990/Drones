package org.nmelnikov.kafkacourse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DroneMetricsMessage {
    private String drone;
    private double temperature;
}
