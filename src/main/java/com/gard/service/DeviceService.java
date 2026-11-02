package com.gard.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import com.gard.dto.DeviceRegisterFailedResponse;
import com.gard.dto.DeviceRegisterRequest;
import com.gard.dto.DeviceRegisteredResponse;
import com.gard.entity.Device;
import com.gard.entity.OutboxMessage;
import com.gard.kafka.KafkaTopics;
import com.gard.repository.DeviceRepository;
import com.gard.repository.OGradRepository;
import com.gard.repository.OutboxMessageRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.ObjectMapper;

@Service
public class DeviceService {

    private static final Logger log = LoggerFactory.getLogger(DeviceService.class);

    private static final String INITIAL_STATE = "enrolling";

    private static final String INITIAL_STATUS = "pending";

    private static final int BATCH_SIZE = 50;

    private final DeviceRepository deviceRepository;
    private final OGradRepository oGradRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;

    private final long chooseWaitSeconds;
    private final long pollIntervalSeconds;
    private final long maxWaitSeconds;
    private final long processingLeaseSeconds;

    public DeviceService(DeviceRepository deviceRepository, OGradRepository oGradRepository,
            OutboxMessageRepository outboxRepository, TransactionTemplate transactionTemplate,
            ObjectMapper objectMapper,
            @Value("${ograd.choose.wait-seconds:60}") long chooseWaitSeconds,
            @Value("${ograd.status.poll-interval-seconds:10}") long pollIntervalSeconds,
            @Value("${ograd.status.max-wait-seconds:300}") long maxWaitSeconds,
            @Value("${device.processing.lease-seconds:180}") long processingLeaseSeconds) {
        this.deviceRepository = deviceRepository;
        this.oGradRepository = oGradRepository;
        this.outboxRepository = outboxRepository;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
        this.chooseWaitSeconds = chooseWaitSeconds;
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.maxWaitSeconds = maxWaitSeconds;
        this.processingLeaseSeconds = processingLeaseSeconds;
    }

    @Transactional
    public void register(DeviceRegisterRequest request) {
        String appReferenceId = request.appReferenceId();
        String imei = request.imei();

        if (appReferenceId == null || appReferenceId.isBlank()) {
            log.warn("Device register command for imei {} rejected: appReferenceId is required", imei);
            reject(appReferenceId, null, imei, "appReferenceId is required");
            return;
        }

        Optional<Device> existingByReference = deviceRepository.findByAppReferenceId(appReferenceId);
        if (existingByReference.isPresent()) {
            log.warn("Device register command for imei {} is a replay of appReferenceId {} (device {}), skipping",
                    imei, appReferenceId, existingByReference.get().getId());
            return;
        }

        registerNewDevice(appReferenceId, imei);
    }

    private void registerNewDevice(String appReferenceId, String imei) {
        Optional<Device> existingByImei = deviceRepository.findByImei(imei);
        if (existingByImei.isPresent()) {
            retryOrReject(appReferenceId, existingByImei.get(), imei);
            return;
        }

        try {
            Device saved = deviceRepository
                    .save(new Device(appReferenceId, imei, null, INITIAL_STATE, INITIAL_STATUS, LocalDateTime.now()));
            log.info("Accepted device register command for imei {} (device {}), OGrad flow pending", imei,
                    saved.getId());
        }
        catch (DataIntegrityViolationException e) {
            Device winner = deviceRepository.findByImei(imei).orElseThrow(() -> e);
            if (appReferenceId.equals(winner.getAppReferenceId())) {
                log.warn("Device register command for imei {} is a replay of appReferenceId {} (device {}), skipping",
                        imei, appReferenceId, winner.getId());
            }
            else {
                retryOrReject(appReferenceId, winner, imei);
            }
        }
    }

    private void retryOrReject(String appReferenceId, Device existing, String imei) {
        if ("failed".equals(existing.getState())) {
            existing.resetForRetry(appReferenceId);
            log.info("Retrying failed registration for imei {} (device {}) with appReferenceId {}", imei,
                    existing.getId(), appReferenceId);
            return;
        }
        log.warn("Device register command for imei {} rejected: already registered as device {}", imei,
                existing.getId());
        reject(appReferenceId, existing.getId(), imei, "imei already registered");
    }

    private boolean claim(Device device, String state) {
        LocalDateTime now = LocalDateTime.now();
        int claimed = deviceRepository.tryClaim(device.getId(), state, now.plusSeconds(processingLeaseSeconds),
                now);
        if (claimed == 0) {
            log.debug("Device {} already claimed by another worker, skipping", device.getId());
            return false;
        }
        return true;
    }

    public void processEnrolling() {
        List<Device> claimableDevices = deviceRepository.findClaimableByState("enrolling", LocalDateTime.now(),
                PageRequest.of(0, BATCH_SIZE));
        for (Device device : claimableDevices) {
            if (!claim(device, "enrolling")) {
                continue;
            }
            String failure = runValidateAndUpload(device);
            try {
                transactionTemplate.executeWithoutResult(
                        transactionStatus -> completeEnrollment(device.getId(), failure));
            }
            catch (OptimisticLockingFailureException e) {
                log.warn("Device {} concurrently updated by another worker, skipping this round", device.getId());
            }
        }
    }

    private String runValidateAndUpload(Device device) {
        try {
            oGradRepository.validateImei(device.getImei());
            oGradRepository.upload(device.getImei());
            return null;
        }
        catch (RuntimeException e) {
            log.warn("OGrad registration failed for device {} imei {}: {}", device.getId(), device.getImei(),
                    e.getMessage());
            return "OGrad registration step failed: " + e.getMessage();
        }
    }

    private void completeEnrollment(String deviceId, String failure) {
        Device device = deviceRepository.findById(deviceId).orElse(null);
        if (device == null || !"enrolling".equals(device.getState())) {
            return;
        }
        if (failure == null) {
            device.markUploaded(LocalDateTime.now().plusSeconds(chooseWaitSeconds));
            log.info("Device {} imei {} validated and uploaded on OGrad, choose due in {}s", device.getId(),
                    device.getImei(), chooseWaitSeconds);
        }
        else {
            failRegistration(device, failure);
        }
    }

    public void processUploaded() {
        LocalDateTime now = LocalDateTime.now();
        List<Device> uploadedDevices = deviceRepository.findClaimableForChoose("uploaded", now, now,
                PageRequest.of(0, BATCH_SIZE));
        for (Device device : uploadedDevices) {
            if (!claim(device, "uploaded")) {
                continue;
            }
            String failure = runChoose(device);
            try {
                transactionTemplate.executeWithoutResult(
                        transactionStatus -> completeChoose(device.getId(), failure));
            }
            catch (OptimisticLockingFailureException e) {
                log.warn("Device {} concurrently updated by another worker, skipping this round", device.getId());
            }
        }
    }

    private String runChoose(Device device) {
        try {
            oGradRepository.choose(device.getImei());
            return null;
        }
        catch (RuntimeException e) {
            log.warn("OGrad choose failed for device {} imei {}: {}", device.getId(), device.getImei(),
                    e.getMessage());
            return "OGrad choose step failed: " + e.getMessage();
        }
    }

    private void completeChoose(String deviceId, String failure) {
        Device device = deviceRepository.findById(deviceId).orElse(null);
        if (device == null || !"uploaded".equals(device.getState())) {
            return;
        }
        if (failure == null) {
            LocalDateTime now = LocalDateTime.now();
            device.markAwaitingStatus(now, now.plusSeconds(maxWaitSeconds));
            log.info("Device {} imei {} package chosen on OGrad, polling status", device.getId(), device.getImei());
        }
        else {
            failRegistration(device, failure);
        }
    }

    public void pollStatus() {
        List<Device> due = deviceRepository
                .findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc("activating", LocalDateTime.now());
        for (Device device : due) {
            String deviceStatus = fetchStatus(device);
            try {
                transactionTemplate.executeWithoutResult(
                        transactionStatus -> completeStatusPoll(device.getId(), deviceStatus));
            }
            catch (OptimisticLockingFailureException e) {
                log.warn("Device {} concurrently updated by another worker, skipping this round", device.getId());
            }
        }
    }

    private String fetchStatus(Device device) {
        try {
            return oGradRepository.getStatus(device.getImei());
        }
        catch (RuntimeException e) {
            log.warn("OGrad status check failed for device {} imei {}: {}", device.getId(), device.getImei(),
                    e.getMessage());
            return null;
        }
    }

    private void completeStatusPoll(String deviceId, String deviceStatus) {
        Device device = deviceRepository.findById(deviceId).orElse(null);
        if (device == null || !"activating".equals(device.getState())) {
            return;
        }
        if ("normal".equals(deviceStatus)) {
            device.markEnrolled(deviceStatus);
            saveResponse(device.getId(), new DeviceRegisteredResponse(device.getAppReferenceId(), device.getId(),
                    device.getImei(), device.getState(), device.getStatus(), "REGISTERED"));
            log.info("Device {} imei {} enrolled via OGrad", device.getId(), device.getImei());
        }
        else if (LocalDateTime.now().isAfter(device.getStatusDeadlineAt())) {
            String lastStatus = deviceStatus != null ? deviceStatus : "unavailable";
            log.warn("OGrad status for device {} imei {} not normal within {}s (last: {})", device.getId(),
                    device.getImei(), maxWaitSeconds, lastStatus);
            failRegistration(device,
                    "OGrad status not normal within " + maxWaitSeconds + "s (last status: " + lastStatus + ")");
        }
        else {
            device.rescheduleStatusCheck(LocalDateTime.now().plusSeconds(pollIntervalSeconds));
        }
    }

    private void reject(String appReferenceId, String deviceId, String imei, String reason) {
        saveResponse(deviceId,
                new DeviceRegisterFailedResponse(appReferenceId, deviceId, imei, "REGISTER_REJECTED", reason));
    }

    private void failRegistration(Device device, String reason) {
        device.markFailed();
        saveResponse(device.getId(), new DeviceRegisterFailedResponse(device.getAppReferenceId(), device.getId(),
                device.getImei(), "REGISTER_FAILED", reason));
    }

    private void saveResponse(String deviceId, Object payload) {
        String message = objectMapper.writeValueAsString(payload);
        outboxRepository.save(new OutboxMessage(KafkaTopics.DEVICE_RESPONSES, deviceId, message, Instant.now()));
    }
}