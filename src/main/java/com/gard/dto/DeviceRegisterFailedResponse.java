package com.gard.dto;

public record DeviceRegisterFailedResponse(String appReferenceId, String deviceId, String imei, String event,
        String reason) {
}