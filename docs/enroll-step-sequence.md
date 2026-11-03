# Enroll Step Consumer — onEnroll

Sequence diagram for the `device-enroll` listener (consumer group `gard-service-enroll`,
`max.poll.records=1`), which drives an `enrolling` device through OGrad `validateImei` + `upload`,
transitions it to `uploaded` and queues the delayed choose step.

```mermaid
sequenceDiagram
    participant Kafka as Kafka (device-enroll)
    participant DeviceStepConsumer
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OGradRepository
    participant OGrad API
    participant OutboxRepository

    Kafka->>DeviceStepConsumer: onEnroll(message)
    Note right of DeviceStepConsumer: manual ack, key = deviceId,<br/>so one consumer owns a device at a time
    DeviceStepConsumer->>DeviceStepConsumer: parse JSON to DeviceStepEvent(deviceId, imei, expectedState, dueAt)
    DeviceStepConsumer->>DeviceService: handleEnroll(event)

    DeviceService->>DeviceRepository: find device by id (pre-check)
    DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
    MariaDB-->>DeviceRepository: device
    DeviceRepository-->>DeviceService: device

    alt device missing or state != 'enrolling'
        Note over DeviceService: stale or duplicate event, skip without calling OGrad
    else state = 'enrolling'
        DeviceService->>OGradRepository: validate imei
        OGradRepository->>OGrad API: validate imei (retried up to 3x)
        OGrad API-->>OGradRepository: ok
        OGradRepository-->>DeviceService: ok
        DeviceService->>OGradRepository: upload
        OGradRepository->>OGrad API: upload (retried up to 3x)
        OGrad API-->>OGradRepository: ok
        OGradRepository-->>DeviceService: ok

        DeviceService->>DeviceService: transition (TransactionTemplate)
        DeviceService->>DeviceRepository: re-fetch device
        DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
        MariaDB-->>DeviceRepository: device
        DeviceRepository-->>DeviceService: device

        alt state changed while OGrad call was running
            Note over DeviceService: skip, another event already advanced the device
        else OGrad steps succeeded
            Note right of DeviceService: markUploaded: state = 'uploaded',<br/>chooseNotBefore = now + 60s
            DeviceService->>OutboxRepository: save DeviceStepEvent(deviceId, imei, 'uploaded', dueAt = chooseNotBefore)
            OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-choose)
            MariaDB-->>OutboxRepository: ok
            OutboxRepository-->>DeviceService: ok
            DeviceService->>MariaDB: UPDATE devices SET state = 'uploaded', ... WHERE id AND version (flush at commit)
            MariaDB-->>DeviceService: ok
        else OGrad step failed after retries
            Note right of DeviceService: markFailed: state = 'failed'
            DeviceService->>OutboxRepository: save REGISTER_FAILED response
            OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-responses)
            MariaDB-->>OutboxRepository: ok
            OutboxRepository-->>DeviceService: ok
            DeviceService->>MariaDB: UPDATE devices SET state = 'failed', ... WHERE id AND version (flush at commit)
            MariaDB-->>DeviceService: ok
        end
        Note right of DeviceService: version conflict (OptimisticLockingFailureException)<br/>is logged and the event skipped
    end

    DeviceService-->>DeviceStepConsumer: ok
    DeviceStepConsumer->>Kafka: acknowledge()
    Note right of Kafka: any other exception: 3 retries (1s apart),<br/>then record goes to device-enroll-dlt
```
