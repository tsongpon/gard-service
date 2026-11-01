package com.gard.consumer;

import com.gard.dto.DeviceRegisterRequest;
import com.gard.kafka.KafkaTopics;
import com.gard.service.DeviceService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

@Component
public class DeviceCommandConsumer {

    public static final String TOPIC = KafkaTopics.DEVICE_COMMANDS;

    private static final Logger log = LoggerFactory.getLogger(DeviceCommandConsumer.class);

    private final DeviceService service;
    private final ObjectMapper objectMapper;

    public DeviceCommandConsumer(DeviceService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = TOPIC)
    public void consumeRegisterDeviceCommand(String message, Acknowledgment acknowledgment) {
        DeviceRegisterRequest request = objectMapper.readValue(message, DeviceRegisterRequest.class);
        log.info("Received device register command for imei {}", request.imei());
        service.register(request);
        acknowledgment.acknowledge();
    }
}