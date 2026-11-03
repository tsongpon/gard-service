package com.gard.kafka;

public final class KafkaTopics {

    public static final String DLT_SUFFIX = "-dlt";

    public static final String DEVICE_COMMANDS = "device-commands";

    public static final String DEVICE_COMMANDS_DLT = DEVICE_COMMANDS + DLT_SUFFIX;

    public static final String DEVICE_RESPONSES = "device-responses";

    public static final String DEVICE_ENROLL = "device-enroll";

    public static final String DEVICE_CHOOSE = "device-choose";

    public static final String DEVICE_STATUS_CHECK = "device-status-check";

    private KafkaTopics() {
    }
}
