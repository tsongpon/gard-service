package com.gard.kafka;

public final class KafkaTopics {

    public static final String DEVICE_COMMANDS = "device-commands";

    public static final String DEVICE_COMMANDS_DLT = DEVICE_COMMANDS + "-dlt";

    public static final String DEVICE_RESPONSES = "device-responses";

    private KafkaTopics() {
    }
}