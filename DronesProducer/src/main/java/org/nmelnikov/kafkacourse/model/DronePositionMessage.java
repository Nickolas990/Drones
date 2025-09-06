package org.nmelnikov.kafkacourse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DronePositionMessage {
    private String drone;
    private double lat;
    private double lon;
}
