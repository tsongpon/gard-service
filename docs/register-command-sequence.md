# Register Command Consumer — consumeRegisterDeviceCommand

Sequence diagram (happy path) for consuming a register command from Kafka
`device-commands`: the consumer parses the message, `DeviceService.register()` saves a new
`enrolling` device and writes its first step event (`device-enroll`) to the outbox in the same
transaction. The outbox publisher then sends it to Kafka (see `outbox-publish-sequence.md`).

```mermaid
sequenceDiagram
    participant Kafka as Kafka (device-commands)
    participant DeviceCommandConsumer
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OutboxRepository

    Kafka->>DeviceCommandConsumer: consumeRegisterDeviceCommand(message)
    Note right of DeviceCommandConsumer: @KafkaListener group gard-service,<br/>manual ack (ack-mode manual_immediate)
    DeviceCommandConsumer->>DeviceCommandConsumer: parse JSON to DeviceRegisterRequest(imei, appReferenceId)
    DeviceCommandConsumer->>DeviceService: register(request)
    Note right of DeviceService: @Transactional

    DeviceService->>DeviceRepository: find by appReferenceId (replay check)
    DeviceRepository->>MariaDB: SELECT devices WHERE app_reference_id = ?
    MariaDB-->>DeviceRepository: none
    DeviceRepository-->>DeviceService: empty

    DeviceService->>DeviceRepository: find by imei (duplicate check)
    DeviceRepository->>MariaDB: SELECT devices WHERE imei = ?
    MariaDB-->>DeviceRepository: none
    DeviceRepository-->>DeviceService: empty

    DeviceService->>DeviceRepository: save new device
    Note right of DeviceRepository: persist only, UUID id assigned,<br/>state = 'enrolling', status = 'pending'
    DeviceRepository-->>DeviceService: saved device

    DeviceService->>OutboxRepository: save DeviceStepEvent(deviceId, imei, 'enrolling', dueAt = null)
    OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-enroll, key = deviceId)
    MariaDB-->>OutboxRepository: ok
    OutboxRepository-->>DeviceService: ok

    DeviceService->>MariaDB: COMMIT
    Note right of MariaDB: INSERT INTO devices flushed at commit,<br/>device row and outbox row become visible together
    MariaDB-->>DeviceService: ok
    DeviceService-->>DeviceCommandConsumer: ok

    DeviceCommandConsumer->>Kafka: acknowledge()
    Note right of Kafka: offset committed only after the transaction succeeded
```
