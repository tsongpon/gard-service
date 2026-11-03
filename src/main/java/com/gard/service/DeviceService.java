package com.gard.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import com.gard.dto.DeviceRegisterFailedResponse;
import com.gard.dto.DeviceRegisterRequest;
import com.gard.dto.DeviceRegisteredResponse;
import com.gard.dto.DeviceStepEvent;
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

    static final String ENROLLING = "enrolling";

    static final String UPLOADED = "uploaded";

    static final String ACTIVATING = "activating";

    static final String FAILED = "failed";

    private static final String INITIAL_STATUS = "pending";

    private static final List<String> IN_FLIGHT_STATES = List.of(ENROLLING, UPLOADED, ACTIVATING);

    private static final int SWEEP_BATCH_SIZE = 50;

    private final DeviceRepository deviceRepository;
    private final OGradRepository oGradRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;

    private final long chooseWaitSeconds;
    private final long pollIntervalSeconds;
    private final long maxWaitSeconds;
    private final long stuckSeconds;

    public DeviceService(DeviceRepository deviceRepository, OGradRepository oGradRepository,
            OutboxMessageRepository outboxRepository, TransactionTemplate transactionTemplate,
            ObjectMapper objectMapper,
            @Value("${ograd.choose.wait-seconds:60}") long chooseWaitSeconds,
            @Value("${ograd.status.poll-interval-seconds:10}") long pollIntervalSeconds,
            @Value("${ograd.status.max-wait-seconds:300}") long maxWaitSeconds,
            @Value("${device.sweeper.stuck-seconds:600}") long stuckSeconds) {
        if (stuckSeconds <= Math.max(chooseWaitSeconds, pollIntervalSeconds)) {
            throw new IllegalStateException("device.sweeper.stuck-seconds (" + stuckSeconds
                    + ") must be greater than ograd.choose.wait-seconds (" + chooseWaitSeconds
                    + ") and ograd.status.poll-interval-seconds (" + pollIntervalSeconds + ")");
        }
        this.deviceRepository = deviceRepository;
        this.oGradRepository = oGradRepository;
        this.outboxRepository = outboxRepository;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
        this.chooseWaitSeconds = chooseWaitSeconds;
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.maxWaitSeconds = maxWaitSeconds;
        this.stuckSeconds = stuckSeconds;
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
                    .save(new Device(appReferenceId, imei, null, ENROLLING, INITIAL_STATUS, LocalDateTime.now()));
            enqueueStep(KafkaTopics.DEVICE_ENROLL, saved, null);
            log.info("Accepted device register command for imei {} (device {}), enroll step queued", imei,
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
        if (FAILED.equals(existing.getState())) {
            existing.resetForRetry(appReferenceId);
            enqueueStep(KafkaTopics.DEVICE_ENROLL, existing, null);
            log.info("Retrying failed registration for imei {} (device {}) with appReferenceId {}", imei,
                    existing.getId(), appReferenceId);
            return;
        }
        log.warn("Device register command for imei {} rejected: already registered as device {}", imei,
                existing.getId());
        reject(appReferenceId, existing.getId(), imei, "imei already registered");
    }

    public void handleEnroll(DeviceStepEvent event) {
        Device device = loadInState(event, ENROLLING);
        if (device == null) {
            return;
        }
        String failure = runValidateAndUpload(device);
        transition(device.getId(), ENROLLING, current -> {
            if (failure != null) {
                failRegistration(current, failure);
                return;
            }
            current.markUploaded();
            enqueueStep(KafkaTopics.DEVICE_CHOOSE, current, Instant.now().plusSeconds(chooseWaitSeconds));
            log.info("Device {} imei {} validated and uploaded on OGrad, choose due in {}s", current.getId(),
                    current.getImei(), chooseWaitSeconds);
        });
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

    public void handleChoose(DeviceStepEvent event) {
        Device device = loadInState(event, UPLOADED);
        if (device == null) {
            return;
        }
        String failure = runChoose(device);
        transition(device.getId(), UPLOADED, current -> {
            if (failure != null) {
                failRegistration(current, failure);
                return;
            }
            current.markAwaitingStatus(LocalDateTime.now().plusSeconds(maxWaitSeconds));
            enqueueStep(KafkaTopics.DEVICE_STATUS_CHECK, current, null);
            log.info("Device {} imei {} package chosen on OGrad, polling status", current.getId(),
                    current.getImei());
        });
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

    public void handleStatusCheck(DeviceStepEvent event) {
        Device device = loadInState(event, ACTIVATING);
        if (device == null) {
            return;
        }
        String deviceStatus = fetchStatus(device);
        transition(device.getId(), ACTIVATING, current -> applyStatus(current, deviceStatus));
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

    private void applyStatus(Device device, String deviceStatus) {
        if ("normal".equals(deviceStatus)) {
            device.markEnrolled(deviceStatus);
            saveResponse(device.getId(), new DeviceRegisteredResponse(device.getAppReferenceId(), device.getId(),
                    device.getImei(), device.getState(), device.getStatus(), "REGISTERED"));
            log.info("Device {} imei {} enrolled via OGrad", device.getId(), device.getImei());
            return;
        }
        if (device.getStatusDeadlineAt() == null || LocalDateTime.now().isAfter(device.getStatusDeadlineAt())) {
            String lastStatus = deviceStatus != null ? deviceStatus : "unavailable";
            log.warn("OGrad status for device {} imei {} not normal within {}s (last: {})", device.getId(),
                    device.getImei(), maxWaitSeconds, lastStatus);
            failRegistration(device,
                    "OGrad status not normal within " + maxWaitSeconds + "s (last status: " + lastStatus + ")");
            return;
        }
        device.touch();
        enqueueStep(KafkaTopics.DEVICE_STATUS_CHECK, device, Instant.now().plusSeconds(pollIntervalSeconds));
    }

    public void sweepStuckDevices() {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(stuckSeconds);
        List<Device> stuck = deviceRepository.findStuck(IN_FLIGHT_STATES, cutoff,
                PageRequest.of(0, SWEEP_BATCH_SIZE));
        for (Device candidate : stuck) {
            transition(candidate.getId(), candidate.getState(), current -> {
                if (!lastChange(current).isBefore(cutoff)) {
                    return;
                }
                current.touch();
                requeueCurrentStep(current);
            });
        }
    }

    private void requeueCurrentStep(Device device) {
        switch (device.getState()) {
            case ENROLLING -> enqueueStep(KafkaTopics.DEVICE_ENROLL, device, null);
            case UPLOADED -> enqueueStep(KafkaTopics.DEVICE_CHOOSE, device, null);
            case ACTIVATING -> enqueueStep(KafkaTopics.DEVICE_STATUS_CHECK, device, null);
            default -> {
                return;
            }
        }
        log.warn("Device {} imei {} stuck in state {}, re-queued its step event", device.getId(), device.getImei(),
                device.getState());
    }

    private static LocalDateTime lastChange(Device device) {
        return device.getUpdatedAt() != null ? device.getUpdatedAt() : device.getCreatedAt();
    }

    private Device loadInState(DeviceStepEvent event, String expectedState) {
        Device device = deviceRepository.findById(event.deviceId()).orElse(null);
        if (device == null || !expectedState.equals(device.getState())) {
            log.info("Ignoring {} event for device {}: current state is {}", expectedState, event.deviceId(),
                    device == null ? "missing" : device.getState());
            return null;
        }
        return device;
    }

    private void transition(String deviceId, String expectedState, Consumer<Device> change) {
        try {
            transactionTemplate.executeWithoutResult(transactionStatus -> {
                Device device = deviceRepository.findById(deviceId).orElse(null);
                if (device == null || !expectedState.equals(device.getState())) {
                    log.info("Device {} left state {} while the step was running, skipping", deviceId,
                            expectedState);
                    return;
                }
                change.accept(device);
            });
        }
        catch (OptimisticLockingFailureException e) {
            log.warn("Device {} concurrently updated, skipping this event", deviceId);
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

    private void enqueueStep(String topic, Device device, Instant dueAt) {
        saveOutbox(topic, device.getId(),
                new DeviceStepEvent(device.getId(), device.getImei(), device.getState(), dueAt));
    }

    private void saveResponse(String deviceId, Object payload) {
        saveOutbox(KafkaTopics.DEVICE_RESPONSES, deviceId, payload);
    }

    private void saveOutbox(String topic, String key, Object payload) {
        String message = objectMapper.writeValueAsString(payload);
        outboxRepository.save(new OutboxMessage(topic, key, message, Instant.now()));
    }
}
