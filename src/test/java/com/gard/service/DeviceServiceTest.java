package com.gard.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import com.gard.dto.DeviceRegisterRequest;
import com.gard.entity.Device;
import com.gard.entity.OutboxMessage;
import com.gard.kafka.KafkaTopics;
import com.gard.repository.DeviceRepository;
import com.gard.repository.OGradRepository;
import com.gard.repository.OutboxMessageRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceServiceTest {

    private DeviceRepository deviceRepository;

    private OGradRepository oGradRepository;

    private OutboxMessageRepository outboxRepository;

    private TransactionTemplate transactionTemplate;

    private DeviceService service;

    @BeforeEach
    void setUp() {
        deviceRepository = mock(DeviceRepository.class);
        oGradRepository = mock(OGradRepository.class);
        outboxRepository = mock(OutboxMessageRepository.class);
        transactionTemplate = mock(TransactionTemplate.class);
        doAnswer(invocation -> {
            Consumer<TransactionStatus> action = invocation.getArgument(0);
            action.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());
        when(deviceRepository.tryClaim(any(), any(), any(), any())).thenReturn(1);
        service = new DeviceService(deviceRepository, oGradRepository, outboxRepository, transactionTemplate,
                new ObjectMapper(), 1, 1, 5, 180);
    }

    @Test
    void registerSavesNewDeviceWhenImeiAndReferenceAreFresh() {
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.empty());
        when(deviceRepository.save(any(Device.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        ArgumentCaptor<Device> captor = ArgumentCaptor.forClass(Device.class);
        verify(deviceRepository).save(captor.capture());
        Device saved = captor.getValue();
        assertThat(saved.getAppReferenceId()).isEqualTo("ref-1");
        assertThat(saved.getImei()).isEqualTo("111");
        assertThat(saved.getState()).isEqualTo("enrolling");
        assertThat(saved.getStatus()).isEqualTo("pending");
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerSkipsReplayOfSameAppReferenceId() {
        Device existing = new Device("ref-1", "111", null, "enrolled", "normal", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.of(existing));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        verify(deviceRepository, never()).save(any(Device.class));
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerRejectsCommandForAlreadyRegisteredImei() {
        Device existing = new Device("ref-old", "111", null, "enrolled", "normal", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-new")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.of(existing));

        service.register(new DeviceRegisterRequest("111", "ref-new"));

        verify(deviceRepository, never()).save(any(Device.class));
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getTopic()).isEqualTo(KafkaTopics.DEVICE_RESPONSES);
        assertThat(saved.getMessageKey()).isEqualTo(existing.getId());
        assertThat(saved.getPayload()).contains("REGISTER_REJECTED").contains("imei already registered")
                .contains("ref-new");
    }

    @Test
    void registerRejectsCommandWithoutAppReferenceId() {
        service.register(new DeviceRegisterRequest("111", " "));

        verify(deviceRepository, never()).save(any(Device.class));
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getPayload()).contains("REGISTER_REJECTED").contains("appReferenceId is required");
    }

    @Test
    void registerTreatsConstraintRaceAsBusinessReject() {
        Device winner = new Device("ref-winner", "111", null, "enrolling", "pending", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-loser")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.empty(), Optional.of(winner));
        when(deviceRepository.save(any(Device.class))).thenThrow(new DataIntegrityViolationException("duplicate"));

        service.register(new DeviceRegisterRequest("111", "ref-loser"));

        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getPayload()).contains("REGISTER_REJECTED").contains("imei already registered")
                .contains("ref-loser");
    }

    @Test
    void registerTreatsConstraintRaceAsReplay() {
        Device winner = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.empty(), Optional.of(winner));
        when(deviceRepository.save(any(Device.class))).thenThrow(new DataIntegrityViolationException("duplicate"));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerResetsFailedDeviceForRetry() {
        Device failed = new Device("ref-old", "111", null, "failed", "pending", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-new")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.of(failed));

        service.register(new DeviceRegisterRequest("111", "ref-new"));

        verify(deviceRepository, never()).save(any(Device.class));
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
        assertThat(failed.getState()).isEqualTo("enrolling");
        assertThat(failed.getStatus()).isEqualTo("pending");
        assertThat(failed.getAppReferenceId()).isEqualTo("ref-new");
        assertThat(failed.getChooseNotBefore()).isNull();
        assertThat(failed.getNextStatusCheckAt()).isNull();
        assertThat(failed.getStatusDeadlineAt()).isNull();
    }

    @Test
    void registerSkipsReplayOfFailedDevice() {
        Device failed = new Device("ref-1", "111", null, "failed", "pending", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.of(failed));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        assertThat(failed.getState()).isEqualTo("failed");
        verify(deviceRepository, never()).save(any(Device.class));
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerTreatsConstraintRaceAsRetryOfFailed() {
        Device winner = new Device("ref-old", "111", null, "failed", "pending", LocalDateTime.now());
        when(deviceRepository.findByAppReferenceId("ref-new")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.empty(), Optional.of(winner));
        when(deviceRepository.save(any(Device.class))).thenThrow(new DataIntegrityViolationException("duplicate"));

        service.register(new DeviceRegisterRequest("111", "ref-new"));

        assertThat(winner.getState()).isEqualTo("enrolling");
        assertThat(winner.getAppReferenceId()).isEqualTo("ref-new");
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void oGradStepFailureMarksDeviceFailedAndWritesErrorResponse() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any())).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        doThrow(new IllegalStateException("connection refused")).when(oGradRepository).validateImei("111");

        service.processEnrolling();

        assertThat(device.getState()).isEqualTo("failed");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getTopic()).isEqualTo(KafkaTopics.DEVICE_RESPONSES);
        assertThat(saved.getMessageKey()).isEqualTo(device.getId());
        assertThat(saved.getPayload()).contains("REGISTER_FAILED").contains("111").contains("connection refused");
    }

    @Test
    void successfulValidateAndUploadMarksDeviceUploaded() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any())).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));

        service.processEnrolling();

        verify(oGradRepository).validateImei("111");
        verify(oGradRepository).upload("111");
        verify(oGradRepository, never()).choose("111");
        assertThat(device.getState()).isEqualTo("uploaded");
        assertThat(device.getChooseNotBefore()).isAfter(LocalDateTime.now());
        assertThat(device.getNextStatusCheckAt()).isNull();
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void processEnrollingSkipsDeviceAlreadyTransitioned() {
        Device device = new Device("ref-1", "111", null, "activating", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any())).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));

        service.processEnrolling();

        verify(oGradRepository).validateImei("111");
        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getNextStatusCheckAt()).isNull();
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void optimisticLockFailureSkipsDeviceAndContinuesBatch() {
        Device first = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        Device second = new Device("ref-2", "222", null, "enrolling", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any())).thenReturn(List.of(first, second));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(second));
        doAnswer(invocation -> {
            throw new OptimisticLockingFailureException("lost race");
        }).doAnswer(invocation -> {
            Consumer<TransactionStatus> action = invocation.getArgument(0);
            action.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        service.processEnrolling();

        assertThat(first.getState()).isEqualTo("enrolling");
        assertThat(second.getState()).isEqualTo("uploaded");
    }

    @Test
    void chooseStepSuccessMarksDeviceActivating() {
        Device device = new Device("ref-1", "111", null, "uploaded", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableForChoose(anyString(), any(LocalDateTime.class), any(), any()))
                .thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));

        service.processUploaded();

        verify(oGradRepository).choose("111");
        verify(oGradRepository, never()).validateImei("111");
        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getStatusDeadlineAt()).isNotNull();
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void chooseStepFailureMarksDeviceFailedAndWritesErrorResponse() {
        Device device = new Device("ref-1", "111", null, "uploaded", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableForChoose(anyString(), any(LocalDateTime.class), any(), any()))
                .thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        doThrow(new IllegalStateException("connection refused")).when(oGradRepository).choose("111");

        service.processUploaded();

        assertThat(device.getState()).isEqualTo("failed");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getPayload()).contains("REGISTER_FAILED").contains("choose step failed");
    }

    @Test
    void processUploadedSkipsDeviceAlreadyTransitioned() {
        Device device = new Device("ref-1", "111", null, "activating", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableForChoose(anyString(), any(LocalDateTime.class), any(), any()))
                .thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));

        service.processUploaded();

        verify(oGradRepository).choose("111");
        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getNextStatusCheckAt()).isNull();
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void optimisticLockFailureDuringChooseSkipsDeviceAndContinuesBatch() {
        Device first = new Device("ref-1", "111", null, "uploaded", "pending", LocalDateTime.now());
        Device second = new Device("ref-2", "222", null, "uploaded", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableForChoose(anyString(), any(LocalDateTime.class), any(), any()))
                .thenReturn(List.of(first, second));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(second));
        doAnswer(invocation -> {
            throw new OptimisticLockingFailureException("lost race");
        }).doAnswer(invocation -> {
            Consumer<TransactionStatus> action = invocation.getArgument(0);
            action.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        service.processUploaded();

        assertThat(first.getState()).isEqualTo("uploaded");
        assertThat(second.getState()).isEqualTo("activating");
    }

    @Test
    void normalStatusEnrollsDeviceAndWritesSuccessResponse() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.markAwaitingStatus(LocalDateTime.now().minusSeconds(2), LocalDateTime.now().plusSeconds(5));
        when(deviceRepository.findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(anyString(),
                any(LocalDateTime.class))).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        when(oGradRepository.getStatus("111")).thenReturn("normal");

        service.pollStatus();

        assertThat(device.getState()).isEqualTo("enrolled");
        assertThat(device.getStatus()).isEqualTo("normal");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getTopic()).isEqualTo(KafkaTopics.DEVICE_RESPONSES);
        assertThat(saved.getPayload()).contains("REGISTERED").contains("ref-1").contains("\"state\":\"enrolled\"")
                .contains("\"status\":\"normal\"");
    }

    @Test
    void overdueDeadlineMarksDeviceFailedAndWritesErrorResponse() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.markAwaitingStatus(LocalDateTime.now().minusSeconds(10), LocalDateTime.now().minusSeconds(1));
        when(deviceRepository.findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(anyString(),
                any(LocalDateTime.class))).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        when(oGradRepository.getStatus("111")).thenReturn("configuring");

        service.pollStatus();

        assertThat(device.getState()).isEqualTo("failed");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getPayload()).contains("REGISTER_FAILED").contains("last status: configuring");
    }

    @Test
    void nonNormalStatusBeforeDeadlineReschedulesCheck() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.markAwaitingStatus(LocalDateTime.now().minusSeconds(2), LocalDateTime.now().plusSeconds(3));
        when(deviceRepository.findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(anyString(),
                any(LocalDateTime.class))).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        when(oGradRepository.getStatus("111")).thenReturn("configuring");

        service.pollStatus();

        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getNextStatusCheckAt()).isAfter(LocalDateTime.now());
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void getStatusFailureBeforeDeadlineReschedulesCheck() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.markAwaitingStatus(LocalDateTime.now().minusSeconds(2), LocalDateTime.now().plusSeconds(3));
        when(deviceRepository.findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(anyString(),
                any(LocalDateTime.class))).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        when(oGradRepository.getStatus("111")).thenThrow(new IllegalStateException("connection refused"));

        service.pollStatus();

        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getNextStatusCheckAt()).isAfter(LocalDateTime.now());
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void getStatusFailureAfterDeadlineMarksDeviceFailed() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.markAwaitingStatus(LocalDateTime.now().minusSeconds(10), LocalDateTime.now().minusSeconds(1));
        when(deviceRepository.findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(anyString(),
                any(LocalDateTime.class))).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        when(oGradRepository.getStatus("111")).thenThrow(new IllegalStateException("connection refused"));

        service.pollStatus();

        assertThat(device.getState()).isEqualTo("failed");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getPayload()).contains("REGISTER_FAILED").contains("unavailable");
    }

    @Test
    void unclaimedDeviceIsSkippedWithoutExternalCall() {
        Device first = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        Device second = new Device("ref-2", "222", null, "enrolling", "pending", LocalDateTime.now());
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any()))
                .thenReturn(List.of(first, second));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(second));
        when(deviceRepository.tryClaim(any(), any(), any(), any())).thenReturn(0, 1);

        service.processEnrolling();

        verify(oGradRepository, never()).validateImei("111");
        verify(oGradRepository).validateImei("222");
        assertThat(first.getState()).isEqualTo("enrolling");
        assertThat(second.getState()).isEqualTo("uploaded");
    }

    @Test
    void successfulValidateAndUploadClearsProcessingLease() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.setProcessingUntil(LocalDateTime.now().plusMinutes(3));
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any())).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));

        service.processEnrolling();

        assertThat(device.getState()).isEqualTo("uploaded");
        assertThat(device.getProcessingUntil()).isNull();
    }

    @Test
    void oGradStepFailureClearsProcessingLease() {
        Device device = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now());
        device.setProcessingUntil(LocalDateTime.now().plusMinutes(3));
        when(deviceRepository.findClaimableByState(eq("enrolling"), any(), any())).thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        doThrow(new IllegalStateException("connection refused")).when(oGradRepository).validateImei("111");

        service.processEnrolling();

        assertThat(device.getState()).isEqualTo("failed");
        assertThat(device.getProcessingUntil()).isNull();
    }

    @Test
    void chooseStepSuccessClearsProcessingLease() {
        Device device = new Device("ref-1", "111", null, "uploaded", "pending", LocalDateTime.now());
        device.setProcessingUntil(LocalDateTime.now().plusMinutes(3));
        when(deviceRepository.findClaimableForChoose(anyString(), any(LocalDateTime.class), any(), any()))
                .thenReturn(List.of(device));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));

        service.processUploaded();

        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getProcessingUntil()).isNull();
    }

    @Test
    void registerRetryClearsStaleLeaseOnFailedDevice() {
        Device failed = new Device("ref-old", "111", null, "failed", "pending", LocalDateTime.now());
        failed.setProcessingUntil(LocalDateTime.now().plusMinutes(3));
        when(deviceRepository.findByAppReferenceId("ref-new")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.of(failed));

        service.register(new DeviceRegisterRequest("111", "ref-new"));

        assertThat(failed.getState()).isEqualTo("enrolling");
        assertThat(failed.getProcessingUntil()).isNull();
    }

    private OutboxMessage captureOutboxSave() {
        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        return captor.getValue();
    }
}