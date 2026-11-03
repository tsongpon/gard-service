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

/**
 * One listener container per workflow step, each in its own consumer group so a slow or paused step never
 * rebalances the others. {@code max.poll.records=1} keeps a poll well within {@code max.poll.interval.ms}
 * even when an OGrad call runs through all its retries.
 *
 * <p>Delayed steps (choose, status check) rely on every event in a topic having the same delay, so events
 * in a partition are already in {@code dueAt} order: only the head record ever needs to wait. When it is not
 * due yet the listener {@code nack}s it, which pauses this container's partitions and re-delivers the same
 * record later without blocking the thread.
 */
@Component
public class DeviceStepConsumer {

    /** Kept well below the default max.poll.interval.ms (5 min); a longer wait just nacks again. */
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

    /** @return true if the event is not due yet and was nacked for re-delivery */
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
