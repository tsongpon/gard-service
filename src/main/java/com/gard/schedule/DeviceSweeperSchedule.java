package com.gard.schedule;

import com.gard.service.DeviceService;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Safety net for the Kafka-driven workflow: re-queues step events for devices that stopped moving. */
@Component
public class DeviceSweeperSchedule {

    private final DeviceService deviceService;

    public DeviceSweeperSchedule(DeviceService deviceService) {
        this.deviceService = deviceService;
    }

    @Scheduled(fixedDelayString = "${device.sweeper.interval-ms:60000}")
    @SchedulerLock(name = "device-stuck-sweeper", lockAtMostFor = "5m", lockAtLeastFor = "1s")
    public void sweep() {
        deviceService.sweepStuckDevices();
    }
}
