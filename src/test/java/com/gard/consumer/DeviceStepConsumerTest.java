package com.gard.consumer;

import java.time.Duration;
import java.time.Instant;

import com.gard.dto.DeviceStepEvent;
import com.gard.service.DeviceService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DeviceStepConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private DeviceService service;

    private Acknowledgment ack;

    private DeviceStepConsumer consumer;

    @BeforeEach
    void setUp() {
        service = mock(DeviceService.class);
        ack = mock(Acknowledgment.class);
        consumer = new DeviceStepConsumer(service, objectMapper);
    }

    @Test
    void enrollRunsImmediatelyAndAcks() {
        consumer.onEnroll(json(null), ack);

        verify(service).handleEnroll(any());
        verify(ack).acknowledge();
    }

    @Test
    void dueChooseEventRunsAndAcks() {
        consumer.onChoose(json(Instant.now().minusSeconds(1)), ack);

        verify(service).handleChoose(any());
        verify(ack).acknowledge();
        verify(ack, never()).nack(any(Duration.class));
    }

    @Test
    void notYetDueChooseEventIsNackedForRemainingWait() {
        consumer.onChoose(json(Instant.now().plusSeconds(5)), ack);

        verify(service, never()).handleChoose(any());
        verify(ack, never()).acknowledge();
        ArgumentCaptor<Duration> wait = ArgumentCaptor.forClass(Duration.class);
        verify(ack).nack(wait.capture());
        assertThat(wait.getValue()).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void longWaitIsCappedSoPollIntervalIsNeverExceeded() {
        consumer.onStatusCheck(json(Instant.now().plusSeconds(600)), ack);

        verify(service, never()).handleStatusCheck(any());
        verify(ack).nack(DeviceStepConsumer.MAX_PAUSE);
    }

    @Test
    void dueStatusCheckRunsAndAcks() {
        consumer.onStatusCheck(json(Instant.now()), ack);

        verify(service).handleStatusCheck(any());
        verify(ack).acknowledge();
    }

    private String json(Instant dueAt) {
        return objectMapper.writeValueAsString(new DeviceStepEvent("dev-1", "111", "uploaded", dueAt));
    }
}
