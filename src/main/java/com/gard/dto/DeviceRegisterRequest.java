package com.gard.dto;

import jakarta.validation.constraints.NotBlank;

public record DeviceRegisterRequest(
        @NotBlank(message = "imei must not be blank") String imei,
        @NotBlank(message = "appReferenceId must not be blank") String appReferenceId) {
}