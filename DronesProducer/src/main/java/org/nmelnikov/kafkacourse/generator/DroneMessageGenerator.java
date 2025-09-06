package org.nmelnikov.kafkacourse.generator;

import com.github.javafaker.Faker;
import org.nmelnikov.kafkacourse.model.*;

import java.util.Locale;

public class DroneMessageGenerator {
    private final Faker faker = new Faker(Locale.of("en", "USA"));

    public DroneMetricsMessage generateMetrics(String droneName) {
        return new DroneMetricsMessage(droneName, faker.number().randomDouble(1, 20, 90));
    }

    public DronePositionMessage generatePosition(String droneName) {
        double lat = Double.parseDouble(faker.address().latitude().replace(",", "."));
        double lon = Double.parseDouble(faker.address().longitude().replace(",", "."));
        return new DronePositionMessage(droneName, lat, lon);
    }

    public DroneDeliveryStatusMessage generateStatus(String droneName) {
        return new DroneDeliveryStatusMessage(droneName, faker.address().cityName());
    }
}
