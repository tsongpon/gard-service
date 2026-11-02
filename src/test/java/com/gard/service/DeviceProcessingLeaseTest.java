package com.gard.service;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;

import com.gard.AbstractIntegrationTest;
import com.gard.entity.Device;
import com.gard.repository.DeviceRepository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DeviceProcessingLeaseTest extends AbstractIntegrationTest {

    @Autowired
    private DeviceRepository repository;

    @Autowired
    private DeviceService deviceService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void activeLeaseSkipsDeviceAndExpiredLeaseProcessesIt() {
        Device device = new Device("ref-lease", "356938035644001", null, "enrolling", "pending",
                LocalDateTime.now());
        device.setProcessingUntil(LocalDateTime.now().plusMinutes(5));
        String deviceId = repository.save(device).getId();

        deviceService.processEnrolling();

        assertThat(repository.findById(deviceId).orElseThrow().getState()).isEqualTo("enrolling");

        jdbcTemplate.update("update devices set processing_until = ? where id = ?",
                Timestamp.valueOf(LocalDateTime.now().minusMinutes(5)), deviceId);

        deviceService.processEnrolling();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> assertThat(repository.findById(deviceId).orElseThrow().getState()).isEqualTo("uploaded"));
    }
}