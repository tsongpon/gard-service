package com.gard.schedule;

import com.gard.service.DeviceService;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DeviceRegistrationSchedule {

    private final DeviceService deviceService;

    public DeviceRegistrationSchedule(DeviceService deviceService) {
        this.deviceService = deviceService;
    }

    @Scheduled(fixedDelay = 5000)
    @SchedulerLock(name = "ograd-registration-worker", lockAtMostFor = "15m", lockAtLeastFor = "1s")
    public void processEnrolling() {
        deviceService.processEnrolling();
    }

    @Scheduled(fixedDelay = 5000)
    @SchedulerLock(name = "ograd-choose-worker", lockAtMostFor = "15m", lockAtLeastFor = "1s")
    public void processUploaded() {
        deviceService.processUploaded();
    }

    @Scheduled(fixedDelay = 5000)
    @SchedulerLock(name = "ograd-status-poller", lockAtMostFor = "10m", lockAtLeastFor = "1s")
    public void pollStatus() {
        deviceService.pollStatus();
    }
}