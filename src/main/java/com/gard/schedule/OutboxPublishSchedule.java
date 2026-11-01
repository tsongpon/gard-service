package com.gard.schedule;

import com.gard.service.OutboxService;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublishSchedule {

    private final OutboxService outboxService;

    public OutboxPublishSchedule(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @Scheduled(fixedDelay = 1000)
    @SchedulerLock(name = "outbox-publisher", lockAtMostFor = "10m", lockAtLeastFor = "1s")
    public void publishPending() {
        outboxService.publishPendingMessages();
    }
}