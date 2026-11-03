package com.gard.config;

import com.gard.kafka.KafkaTopics;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import tools.jackson.core.JacksonException;

@Configuration
public class KafkaListenerConfig {

    /**
     * Shared by all listeners: retries 3 times, then dead-letters to {@code <source-topic>-dlt} on the same
     * partition. Malformed JSON goes straight to the DLT. Device-step events that land in a DLT are
     * recovered later by the stuck-device sweeper.
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + KafkaTopics.DLT_SUFFIX,
                        record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }
}
