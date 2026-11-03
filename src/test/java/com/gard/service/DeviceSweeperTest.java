package com.gard.service;

import java.time.Duration;
import java.time.LocalDateTime;

import com.gard.AbstractIntegrationTest;
import com.gard.entity.Device;
import com.gard.repository.DeviceRepository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A device left in flight with no step event (e.g. created by the old scheduler-based version, or its event
 * was dead-lettered) is picked up by the sweeper and driven to completion through Kafka.
 */
class DeviceSweeperTest extends AbstractIntegrationTest {

    @Autowired
    private DeviceRepository repository;

    @Autowired
    private DeviceService deviceService;

    @Test
    void stuckEnrollingDeviceIsRequeuedAndCompletesFlow() {
        Device orphan = new Device("ref-sweep", "356938035644001", null, "enrolling", "pending",
                LocalDateTime.now().minusHours(1));
        String deviceId = repository.save(orphan).getId();

        deviceService.sweepStuckDevices();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(repository.findById(deviceId).orElseThrow().getState()).isEqualTo("enrolled"));
    }

    @Test
    void recentlyUpdatedDeviceIsNotRequeued() {
        Device fresh = new Device("ref-fresh", "356938035644002", null, "enrolling", "pending",
                LocalDateTime.now());
        String deviceId = repository.save(fresh).getId();

        deviceService.sweepStuckDevices();

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).untilAsserted(
                () -> assertThat(repository.findById(deviceId).orElseThrow().getState()).isEqualTo("enrolling"));
    }
}
