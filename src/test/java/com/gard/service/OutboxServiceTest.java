package com.gard.service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.gard.entity.OutboxMessage;
import com.gard.repository.OutboxMessageRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxServiceTest {

    private OutboxMessageRepository repository;

    private KafkaTemplate<Object, Object> kafkaTemplate;

    private OutboxService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(OutboxMessageRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        service = new OutboxService(repository, kafkaTemplate);
    }

    @Test
    void publishesAndDeletesAllPending() {
        OutboxMessage first = new OutboxMessage("device-responses", "k1", "payload-1", Instant.now());
        OutboxMessage second = new OutboxMessage("device-responses", "k2", "payload-2", Instant.now());
        when(repository.findTop50ByOrderByIdAsc()).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        service.publishPendingMessages();

        verify(repository).delete(first);
        verify(repository).delete(second);
    }

    @Test
    void sendFailureStopsBatchSoLaterMessagesAreNotPublishedOutOfOrder() {
        OutboxMessage failing = new OutboxMessage("device-responses", "k1", "payload-1", Instant.now());
        OutboxMessage later = new OutboxMessage("device-responses", "k1", "payload-2", Instant.now());
        when(repository.findTop50ByOrderByIdAsc()).thenReturn(List.of(failing, later));
        CompletableFuture<SendResult<Object, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new ExecutionException(new IllegalStateException("broker down")));
        when(kafkaTemplate.send(anyString(), any(), any())).thenReturn(failed);

        service.publishPendingMessages();

        verify(kafkaTemplate, times(1)).send(anyString(), any(), any());
        verify(repository, never()).delete(any(OutboxMessage.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void interruptAbortsBatchWithoutDeleting() throws Exception {
        OutboxMessage message = new OutboxMessage("device-responses", "k1", "payload-1", Instant.now());
        when(repository.findTop50ByOrderByIdAsc()).thenReturn(List.of(message));
        CompletableFuture<SendResult<Object, Object>> future = mock(CompletableFuture.class);
        when(kafkaTemplate.send(anyString(), any(), any())).thenReturn(future);
        when(future.get(anyLong(), any())).thenThrow(new InterruptedException());

        assertThatThrownBy(() -> service.publishPendingMessages()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Interrupted");

        assertThat(Thread.interrupted()).isTrue();
        verify(repository, never()).delete(any(OutboxMessage.class));
    }
}