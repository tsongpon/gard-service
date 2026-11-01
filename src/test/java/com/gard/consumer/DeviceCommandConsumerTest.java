package com.gard.consumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.gard.AbstractIntegrationTest;
import com.gard.dto.DeviceRegisterFailedResponse;
import com.gard.dto.DeviceRegisteredResponse;
import com.gard.entity.Device;
import com.gard.kafka.KafkaTopics;
import com.gard.repository.DeviceRepository;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DeviceCommandConsumerTest extends AbstractIntegrationTest {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private DeviceRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void consumesRegisterDeviceCommand() throws Exception {
        kafkaTemplate.send(DeviceCommandConsumer.TOPIC, "{\"imei\":\"356938035643809\",\"appReferenceId\":\"ref-a\"}")
                .get(5, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var device = repository.findByImei("356938035643809");
            assertThat(device).isPresent();
            assertThat(device.get().getId()).isNotNull();
            assertThat(device.get().getAppReferenceId()).isEqualTo("ref-a");
            assertThat(device.get().getState()).isEqualTo("enrolled");
            assertThat(device.get().getStatus()).isEqualTo("normal");
            assertThat(device.get().getAccountNumber()).isNull();
            assertThat(device.get().getCreatedAt()).isNotNull();
        });
    }

    @Test
    void producesResponseToResponseTopicAfterCommand() throws Exception {
        kafkaTemplate.send(DeviceCommandConsumer.TOPIC, "{\"imei\":\"356938035643810\",\"appReferenceId\":\"ref-b\"}")
                .get(5, TimeUnit.SECONDS);

        AtomicReference<DeviceRegisteredResponse> received = new AtomicReference<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "device-outbox-test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(KafkaTopics.DEVICE_RESPONSES));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    DeviceRegisteredResponse response = objectMapper.readValue(record.value(),
                            DeviceRegisteredResponse.class);
                    if ("ref-b".equals(response.appReferenceId())) {
                        received.set(response);
                    }
                });
                assertThat(received.get()).isNotNull();
                assertThat(received.get().deviceId()).isNotNull();
                assertThat(received.get().state()).isEqualTo("enrolled");
                assertThat(received.get().status()).isEqualTo("normal");
                assertThat(received.get().event()).isEqualTo("REGISTERED");
            });
        }
    }

    @Test
    void resendAfterFailedRegistrationRetriesFlow() throws Exception {
        Device failed = new Device("ref-old", "356938035643812", null, "failed", "pending", LocalDateTime.now());
        String deviceId = repository.save(failed).getId();

        kafkaTemplate.send(DeviceCommandConsumer.TOPIC,
                "{\"imei\":\"356938035643812\",\"appReferenceId\":\"ref-retry\"}").get(5, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var device = repository.findByImei("356938035643812");
            assertThat(device).isPresent();
            assertThat(device.get().getId()).isEqualTo(deviceId);
            assertThat(device.get().getAppReferenceId()).isEqualTo("ref-retry");
            assertThat(device.get().getState()).isEqualTo("enrolled");
            assertThat(device.get().getStatus()).isEqualTo("normal");
        });
    }

    @Test
    void sendsMalformedCommandToDeadLetterTopicAndContinuesProcessing() throws Exception {
        String malformed = "{\"imei\":";
        kafkaTemplate.send(DeviceCommandConsumer.TOPIC, malformed).get(5, TimeUnit.SECONDS);

        AtomicReference<String> received = new AtomicReference<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "device-dlt-test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(KafkaTopics.DEVICE_COMMANDS_DLT));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> received.set(record.value()));
                assertThat(received.get()).isEqualTo(malformed);
            });
        }

        kafkaTemplate.send(DeviceCommandConsumer.TOPIC, "{\"imei\":\"356938035643813\",\"appReferenceId\":\"ref-e\"}")
                .get(5, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var device = repository.findByImei("356938035643813");
            assertThat(device).isPresent();
            assertThat(device.get().getAppReferenceId()).isEqualTo("ref-e");
        });
    }

    @Test
    void rejectsRegisterCommandForAlreadyRegisteredImei() throws Exception {
        kafkaTemplate.send(DeviceCommandConsumer.TOPIC,
                "{\"imei\":\"356938035643811\",\"appReferenceId\":\"ref-c\"}").get(5, TimeUnit.SECONDS);
        kafkaTemplate.send(DeviceCommandConsumer.TOPIC,
                "{\"imei\":\"356938035643811\",\"appReferenceId\":\"ref-d\"}").get(5, TimeUnit.SECONDS);

        AtomicReference<DeviceRegisterFailedResponse> received = new AtomicReference<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "device-outbox-test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(KafkaTopics.DEVICE_RESPONSES));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    DeviceRegisterFailedResponse response = objectMapper.readValue(record.value(),
                            DeviceRegisterFailedResponse.class);
                    if ("ref-d".equals(response.appReferenceId())) {
                        received.set(response);
                    }
                });
                assertThat(received.get()).isNotNull();
                assertThat(received.get().deviceId()).isNotNull();
                assertThat(received.get().imei()).isEqualTo("356938035643811");
                assertThat(received.get().event()).isEqualTo("REGISTER_REJECTED");
                assertThat(received.get().reason()).contains("already registered");
            });
        }
    }
}