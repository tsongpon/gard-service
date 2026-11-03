package com.gard.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import com.gard.dto.DeviceRegisterRequest;
import com.gard.dto.DeviceStepEvent;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DeviceServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

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
        service = new DeviceService(deviceRepository, oGradRepository, outboxRepository, transactionTemplate,
                objectMapper, 1, 1, 5, 600);
    }

    @Test
    void registerSavesNewDeviceAndQueuesEnrollStep() {
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
        assertThat(saved.getUpdatedAt()).isNotNull();

        OutboxMessage queued = captureOutboxSave();
        assertThat(queued.getTopic()).isEqualTo(KafkaTopics.DEVICE_ENROLL);
        DeviceStepEvent event = readEvent(queued);
        assertThat(event.imei()).isEqualTo("111");
        assertThat(event.expectedState()).isEqualTo("enrolling");
        assertThat(event.dueAt()).isNull();
    }

    @Test
    void registerSkipsReplayOfSameAppReferenceId() {
        Device existing = device("ref-1", "111", "enrolled");
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.of(existing));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        verify(deviceRepository, never()).save(any(Device.class));
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerRejectsCommandForAlreadyRegisteredImei() {
        Device existing = device("ref-old", "111", "enrolled");
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
        Device winner = device("ref-winner", "111", "enrolling");
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
        Device winner = device("ref-1", "111", "enrolling");
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.empty(), Optional.of(winner));
        when(deviceRepository.save(any(Device.class))).thenThrow(new DataIntegrityViolationException("duplicate"));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerResetsFailedDeviceAndQueuesEnrollStep() {
        Device failed = device("ref-old", "111", "failed");
        when(deviceRepository.findByAppReferenceId("ref-new")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.of(failed));

        service.register(new DeviceRegisterRequest("111", "ref-new"));

        verify(deviceRepository, never()).save(any(Device.class));
        assertThat(failed.getState()).isEqualTo("enrolling");
        assertThat(failed.getStatus()).isEqualTo("pending");
        assertThat(failed.getAppReferenceId()).isEqualTo("ref-new");
        assertThat(failed.getChooseNotBefore()).isNull();
        assertThat(failed.getNextStatusCheckAt()).isNull();
        assertThat(failed.getStatusDeadlineAt()).isNull();
        assertThat(captureOutboxSave().getTopic()).isEqualTo(KafkaTopics.DEVICE_ENROLL);
    }

    @Test
    void registerSkipsReplayOfFailedDevice() {
        Device failed = device("ref-1", "111", "failed");
        when(deviceRepository.findByAppReferenceId("ref-1")).thenReturn(Optional.of(failed));

        service.register(new DeviceRegisterRequest("111", "ref-1"));

        assertThat(failed.getState()).isEqualTo("failed");
        verify(deviceRepository, never()).save(any(Device.class));
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void registerTreatsConstraintRaceAsRetryOfFailed() {
        Device winner = device("ref-old", "111", "failed");
        when(deviceRepository.findByAppReferenceId("ref-new")).thenReturn(Optional.empty());
        when(deviceRepository.findByImei("111")).thenReturn(Optional.empty(), Optional.of(winner));
        when(deviceRepository.save(any(Device.class))).thenThrow(new DataIntegrityViolationException("duplicate"));

        service.register(new DeviceRegisterRequest("111", "ref-new"));

        assertThat(winner.getState()).isEqualTo("enrolling");
        assertThat(winner.getAppReferenceId()).isEqualTo("ref-new");
        assertThat(captureOutboxSave().getTopic()).isEqualTo(KafkaTopics.DEVICE_ENROLL);
    }

    @Test
    void enrollSuccessMarksUploadedAndQueuesDelayedChoose() {
        Device device = stored(device("ref-1", "111", "enrolling"));

        service.handleEnroll(event("enrolling"));

        verify(oGradRepository).validateImei("111");
        verify(oGradRepository).upload("111");
        verify(oGradRepository, never()).choose("111");
        assertThat(device.getState()).isEqualTo("uploaded");
        assertThat(device.getChooseNotBefore()).isAfter(LocalDateTime.now());

        OutboxMessage queued = captureOutboxSave();
        assertThat(queued.getTopic()).isEqualTo(KafkaTopics.DEVICE_CHOOSE);
        DeviceStepEvent next = readEvent(queued);
        assertThat(next.expectedState()).isEqualTo("uploaded");
        assertThat(next.dueAt()).isAfter(Instant.now());
    }

    @Test
    void enrollFailureMarksDeviceFailedAndWritesErrorResponse() {
        Device device = stored(device("ref-1", "111", "enrolling"));
        doThrow(new IllegalStateException("connection refused")).when(oGradRepository).validateImei("111");

        service.handleEnroll(event("enrolling"));

        assertThat(device.getState()).isEqualTo("failed");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getTopic()).isEqualTo(KafkaTopics.DEVICE_RESPONSES);
        assertThat(saved.getPayload()).contains("REGISTER_FAILED").contains("111").contains("connection refused");
    }

    @Test
    void staleEnrollEventIsIgnoredWithoutCallingOGrad() {
        Device device = stored(device("ref-1", "111", "uploaded"));

        service.handleEnroll(event("enrolling"));

        verifyNoInteractions(oGradRepository);
        assertThat(device.getState()).isEqualTo("uploaded");
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void enrollEventForMissingDeviceIsIgnored() {
        when(deviceRepository.findById(any())).thenReturn(Optional.empty());

        service.handleEnroll(event("enrolling"));

        verifyNoInteractions(oGradRepository);
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void enrollSkipsTransitionWhenStateChangedDuringOGradCall() {
        Device before = device("ref-1", "111", "enrolling");
        Device after = device("ref-1", "111", "failed");
        when(deviceRepository.findById(any())).thenReturn(Optional.of(before), Optional.of(after));

        service.handleEnroll(event("enrolling"));

        verify(oGradRepository).upload("111");
        assertThat(after.getState()).isEqualTo("failed");
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void optimisticLockFailureIsSwallowed() {
        stored(device("ref-1", "111", "enrolling"));
        doThrow(new OptimisticLockingFailureException("lost race")).when(transactionTemplate)
                .executeWithoutResult(any());

        service.handleEnroll(event("enrolling"));

        verify(oGradRepository).upload("111");
    }

    @Test
    void chooseSuccessMarksActivatingAndQueuesStatusCheck() {
        Device device = stored(device("ref-1", "111", "uploaded"));

        service.handleChoose(event("uploaded"));

        verify(oGradRepository).choose("111");
        verify(oGradRepository, never()).validateImei("111");
        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getStatusDeadlineAt()).isAfter(LocalDateTime.now());

        OutboxMessage queued = captureOutboxSave();
        assertThat(queued.getTopic()).isEqualTo(KafkaTopics.DEVICE_STATUS_CHECK);
        assertThat(readEvent(queued).expectedState()).isEqualTo("activating");
    }

    @Test
    void chooseFailureMarksDeviceFailedAndWritesErrorResponse() {
        Device device = stored(device("ref-1", "111", "uploaded"));
        doThrow(new IllegalStateException("connection refused")).when(oGradRepository).choose("111");

        service.handleChoose(event("uploaded"));

        assertThat(device.getState()).isEqualTo("failed");
        assertThat(captureOutboxSave().getPayload()).contains("REGISTER_FAILED").contains("choose step failed");
    }

    @Test
    void staleChooseEventIsIgnoredWithoutCallingOGrad() {
        stored(device("ref-1", "111", "activating"));

        service.handleChoose(event("uploaded"));

        verifyNoInteractions(oGradRepository);
        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    @Test
    void normalStatusEnrollsDeviceAndWritesSuccessResponse() {
        Device device = stored(activating(LocalDateTime.now().plusSeconds(5)));
        when(oGradRepository.getStatus("111")).thenReturn("normal");

        service.handleStatusCheck(event("activating"));

        assertThat(device.getState()).isEqualTo("enrolled");
        assertThat(device.getStatus()).isEqualTo("normal");
        OutboxMessage saved = captureOutboxSave();
        assertThat(saved.getTopic()).isEqualTo(KafkaTopics.DEVICE_RESPONSES);
        assertThat(saved.getPayload()).contains("REGISTERED").contains("ref-1").contains("\"state\":\"enrolled\"")
                .contains("\"status\":\"normal\"");
    }

    @Test
    void nonNormalStatusBeforeDeadlineQueuesNextCheck() {
        Device device = stored(activating(LocalDateTime.now().plusSeconds(3)));
        when(oGradRepository.getStatus("111")).thenReturn("configuring");

        service.handleStatusCheck(event("activating"));

        assertThat(device.getState()).isEqualTo("activating");
        assertThat(device.getNextStatusCheckAt()).isAfter(LocalDateTime.now());
        OutboxMessage queued = captureOutboxSave();
        assertThat(queued.getTopic()).isEqualTo(KafkaTopics.DEVICE_STATUS_CHECK);
        assertThat(readEvent(queued).dueAt()).isAfter(Instant.now());
    }

    @Test
    void getStatusFailureBeforeDeadlineQueuesNextCheck() {
        Device device = stored(activating(LocalDateTime.now().plusSeconds(3)));
        when(oGradRepository.getStatus("111")).thenThrow(new IllegalStateException("connection refused"));

        service.handleStatusCheck(event("activating"));

        assertThat(device.getState()).isEqualTo("activating");
        assertThat(captureOutboxSave().getTopic()).isEqualTo(KafkaTopics.DEVICE_STATUS_CHECK);
    }

    @Test
    void overdueDeadlineMarksDeviceFailedAndWritesErrorResponse() {
        Device device = stored(activating(LocalDateTime.now().minusSeconds(1)));
        when(oGradRepository.getStatus("111")).thenReturn("configuring");

        service.handleStatusCheck(event("activating"));

        assertThat(device.getState()).isEqualTo("failed");
        assertThat(captureOutboxSave().getPayload()).contains("REGISTER_FAILED").contains("last status: configuring");
    }

    @Test
    void getStatusFailureAfterDeadlineMarksDeviceFailed() {
        Device device = stored(activating(LocalDateTime.now().minusSeconds(1)));
        when(oGradRepository.getStatus("111")).thenThrow(new IllegalStateException("connection refused"));

        service.handleStatusCheck(event("activating"));

        assertThat(device.getState()).isEqualTo("failed");
        assertThat(captureOutboxSave().getPayload()).contains("REGISTER_FAILED").contains("unavailable");
    }

    @Test
    void sweeperRequeuesStepForEachStuckState() {
        Device enrolling = new Device("ref-1", "111", null, "enrolling", "pending", LocalDateTime.now().minusHours(1));
        Device uploaded = new Device("ref-2", "222", null, "enrolling", "pending", LocalDateTime.now().minusHours(1));
        uploaded.markUploaded(LocalDateTime.now().minusMinutes(50));
        uploaded.setUpdatedAt(LocalDateTime.now().minusHours(1));
        when(deviceRepository.findStuck(any(), any(), any())).thenReturn(List.of(enrolling, uploaded));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(enrolling), Optional.of(uploaded));

        service.sweepStuckDevices();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, org.mockito.Mockito.times(2)).save(captor.capture());
        assertThat(captor.getAllValues()).extracting(OutboxMessage::getTopic)
                .containsExactly(KafkaTopics.DEVICE_ENROLL, KafkaTopics.DEVICE_CHOOSE);
        assertThat(enrolling.getUpdatedAt()).isAfter(LocalDateTime.now().minusMinutes(1));
        assertThat(uploaded.getUpdatedAt()).isAfter(LocalDateTime.now().minusMinutes(1));
    }

    @Test
    void sweeperSkipsDeviceThatMovedSinceTheQuery() {
        Device stale = new Device("ref-1", "111", null, "activating", "pending", LocalDateTime.now().minusHours(1));
        Device fresh = new Device("ref-1", "111", null, "activating", "pending", LocalDateTime.now());
        when(deviceRepository.findStuck(any(), any(), any())).thenReturn(List.of(stale));
        when(deviceRepository.findById(any())).thenReturn(Optional.of(fresh));

        service.sweepStuckDevices();

        verify(outboxRepository, never()).save(any(OutboxMessage.class));
    }

    private static Device device(String reference, String imei, String state) {
        return new Device(reference, imei, null, state, "pending", LocalDateTime.now());
    }

    private static Device activating(LocalDateTime deadline) {
        Device device = device("ref-1", "111", "enrolling");
        device.markAwaitingStatus(LocalDateTime.now(), deadline);
        return device;
    }

    private Device stored(Device device) {
        when(deviceRepository.findById(any())).thenReturn(Optional.of(device));
        return device;
    }

    private static DeviceStepEvent event(String state) {
        return new DeviceStepEvent("dev-1", "111", state, null);
    }

    private DeviceStepEvent readEvent(OutboxMessage message) {
        return objectMapper.readValue(message.getPayload(), DeviceStepEvent.class);
    }

    private OutboxMessage captureOutboxSave() {
        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        return captor.getValue();
    }
}
