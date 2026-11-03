# Register Command Consumer — consumeRegisterDeviceCommand

Sequence diagram (happy path) for consuming a register command from Kafka
`device-commands`: the consumer parses the message and `DeviceService.register()`
saves a new `enrolling` device and queues its first step event (see kafka-workflow.md).

```mermaid
sequenceDiagram
    participant K as Kafka (device-commands)
    participant C as DeviceCommandConsumer
    participant S as DeviceService (register transaction)
    participant R as DeviceRepository
    participant DB as MariaDB
    participant P as OutboxService

    K->>C: consumeRegisterDeviceCommand(message)
    Note right of C: @KafkaListener group gard-service,<br/>manual ack (ack-mode manual_immediate)
    C->>C: parse JSON to DeviceRegisterRequest(imei, appReferenceId)
    C->>S: register(request) - @Transactional
    S->>R: findByAppReferenceId (replay check)
    R->>DB: SELECT devices WHERE app_reference_id = ?
    DB-->>R: none
    S->>R: findByImei (duplicate check)
    R->>DB: SELECT devices WHERE imei = ?
    DB-->>R: none
    S->>R: save new Device (persist only, UUID assigned, no SQL yet)
    S->>S: write DeviceStepEvent to outbox (device-enroll)
    S-->>C: return, transaction commits
    Note over S,DB: INSERT devices (state = 'enrolling') and INSERT outbox_messages<br/>run in the same transaction and become visible together at commit
    C->>K: acknowledge() - offset committed after success
    P->>K: publish device-enroll event (outbox poll, every 1s)
    Note over P: enroll consumer continues the flow (see kafka-workflow.md);<br/>a response reaches device-responses once the device is enrolled or failed
```