package com.gard.config;

import com.gard.kafka.KafkaTopics;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares every topic the service uses so they exist before listeners start. Each DLT gets the same
 * partition count as its source topic, because the dead-letter recoverer keeps the original partition.
 */
@Configuration
public class KafkaTopicConfig {

    private final int partitions;

    public KafkaTopicConfig(@Value("${device.kafka.partitions:3}") int partitions) {
        this.partitions = partitions;
    }

    @Bean
    public KafkaAdmin.NewTopics gardTopics() {
        return new KafkaAdmin.NewTopics(
                topic(KafkaTopics.DEVICE_COMMANDS), topic(KafkaTopics.DEVICE_COMMANDS_DLT),
                topic(KafkaTopics.DEVICE_RESPONSES),
                topic(KafkaTopics.DEVICE_ENROLL), topic(KafkaTopics.DEVICE_ENROLL + KafkaTopics.DLT_SUFFIX),
                topic(KafkaTopics.DEVICE_CHOOSE), topic(KafkaTopics.DEVICE_CHOOSE + KafkaTopics.DLT_SUFFIX),
                topic(KafkaTopics.DEVICE_STATUS_CHECK),
                topic(KafkaTopics.DEVICE_STATUS_CHECK + KafkaTopics.DLT_SUFFIX));
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(partitions).build();
    }
}
