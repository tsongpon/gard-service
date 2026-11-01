package com.gard.dto;

public record DeviceRegisteredResponse(String appReferenceId, String deviceId, String imei, String state,
        String status, String event) {
}