package com.gard.service;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.gard.entity.OutboxMessage;
import com.gard.repository.OutboxMessageRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OutboxService {

    private static final Logger log = LoggerFactory.getLogger(OutboxService.class);

    private final OutboxMessageRepository repository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OutboxService(OutboxMessageRepository repository, KafkaTemplate<Object, Object> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPendingMessages() {
        List<OutboxMessage> pending = repository.findTop50ByOrderByIdAsc();
        int published = 0;
        for (OutboxMessage message : pending) {
            try {
                kafkaTemplate.send(message.getTopic(), message.getMessageKey(), message.getPayload())
                        .get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while publishing outbox message " + message.getId(), e);
            }
            catch (ExecutionException | TimeoutException e) {
                // Stop here rather than skip ahead: later rows may be for the same device, and publishing them
                // first would deliver that device's events out of order.
                log.warn("Failed to publish outbox message {}, stopping batch and retrying next cycle: {}",
                        message.getId(), e.getMessage());
                break;
            }
            repository.delete(message);
            published++;
        }
        if (published > 0) {
            log.info("Published {} outbox message(s) to Kafka", published);
        }
    }
}