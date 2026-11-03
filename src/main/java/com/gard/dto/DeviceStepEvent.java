package com.gard.dto;

import java.time.Instant;

/**
 * Internal workflow event telling a step consumer to advance a device. The database stays the source of
 * truth: the consumer re-reads the device and only acts if it is still in {@code expectedState}, so
 * duplicate or stale events are harmless.
 *
 * @param dueAt earliest time the step may run; {@code null} means immediately
 */
public record DeviceStepEvent(String deviceId, String imei, String expectedState, Instant dueAt) {
}
