# Register Command Consumer — consumeRegisterDeviceCommand

Sequence diagram (happy path) for consuming a register command from Kafka
`device-commands`: the consumer parses the message and `DeviceService.register()`
saves a new `enrolling` device for the registration workers to pick up.

```mermaid
sequenceDiagram
    participant K as Kafka (device-commands)
    participant C as DeviceCommandConsumer
    participant S as DeviceService (register transaction)
    participant R as DeviceRepository
    participant DB as MariaDB

    K->>C: consumeRegisterDeviceCommand(message)
    Note right of C: @KafkaListener group gard-service,<br/>manual ack (ack-mode manual_immediate)
    C->>C: parse JSON to DeviceRegisterRequest(imei, appReferenceId)
    C->>S: register(request) - @Transactional
    S->>R: save new Device
    R->>DB: INSERT INTO devices (state = 'enrolling', status = 'pending')
    DB-->>R: ok
    R-->>S: saved
    S-->>C: commit transaction
    C->>K: acknowledge() - offset committed after success
    Note over S: device picked up by processEnrolling worker<br/>within 5s (see process-enrolling-sequence.md),<br/>response event flows to device-responses via outbox once enrolled
```