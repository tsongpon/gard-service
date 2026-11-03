package com.gard.kafka;

public final class KafkaTopics {

    public static final String DLT_SUFFIX = "-dlt";

    /** Inbound register commands from the app. */
    public static final String DEVICE_COMMANDS = "device-commands";

    public static final String DEVICE_COMMANDS_DLT = DEVICE_COMMANDS + DLT_SUFFIX;

    /** Outbound results (REGISTERED / REGISTER_FAILED / REGISTER_REJECTED) for the app. */
    public static final String DEVICE_RESPONSES = "device-responses";

    /** Step event: device is {@code enrolling}, run OGrad validateImei + upload. */
    public static final String DEVICE_ENROLL = "device-enroll";

    /** Step event (delayed until {@code dueAt}): device is {@code uploaded}, run OGrad choose. */
    public static final String DEVICE_CHOOSE = "device-choose";

    /** Step event (delayed until {@code dueAt}): device is {@code activating}, poll OGrad status. */
    public static final String DEVICE_STATUS_CHECK = "device-status-check";

    private KafkaTopics() {
    }
}
