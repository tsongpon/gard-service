package com.gard.dto;

import java.time.Instant;

public record DeviceStepEvent(String deviceId, String imei, String expectedState, Instant dueAt) {
}
