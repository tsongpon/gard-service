package com.gard.consumer;

import java.time.Duration;
import java.time.Instant;

import com.gard.dto.DeviceStepEvent;
import com.gard.kafka.KafkaTopics;
import com.gard.service.DeviceService;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

@Component
public class DeviceStepConsumer {

    static final Duration MAX_PAUSE = Duration.ofSeconds(30);

    private final DeviceService service;
    private final ObjectMapper objectMapper;

    public DeviceStepConsumer(DeviceService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_ENROLL, groupId = "gard-service-enroll",
            concurrency = "${device.step.concurrency:3}", properties = "max.poll.records=1")
    public void onEnroll(String message, Acknowledgment ack) {
        service.handleEnroll(parse(message));
        ack.acknowledge();
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_CHOOSE, groupId = "gard-service-choose",
            concurrency = "${device.step.concurrency:3}", properties = "max.poll.records=1")
    public void onChoose(String message, Acknowledgment ack) {
        DeviceStepEvent event = parse(message);
        if (deferUntilDue(event, ack)) {
            return;
        }
        service.handleChoose(event);
        ack.acknowledge();
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_STATUS_CHECK, groupId = "gard-service-status-check",
            concurrency = "${device.step.concurrency:3}", properties = "max.poll.records=1")
    public void onStatusCheck(String message, Acknowledgment ack) {
        DeviceStepEvent event = parse(message);
        if (deferUntilDue(event, ack)) {
            return;
        }
        service.handleStatusCheck(event);
        ack.acknowledge();
    }

    // All events in a delay topic share one delay, so only the partition head ever waits. nack pauses the
    // partition instead of blocking the thread; the cap keeps each pause under max.poll.interval.ms.
    static boolean deferUntilDue(DeviceStepEvent event, Acknowledgment ack) {
        if (event.dueAt() == null) {
            return false;
        }
        Duration wait = Duration.between(Instant.now(), event.dueAt());
        if (wait.isNegative() || wait.isZero()) {
            return false;
        }
        ack.nack(wait.compareTo(MAX_PAUSE) > 0 ? MAX_PAUSE : wait);
        return true;
    }

    private DeviceStepEvent parse(String message) {
        return objectMapper.readValue(message, DeviceStepEvent.class);
    }
}
