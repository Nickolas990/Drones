package org.nmelnikov.kafkacourse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DroneDeliveryStatusMessage {
    private String drone;
    private String destination;
}
