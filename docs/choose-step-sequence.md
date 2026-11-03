# Choose Step Consumer — onChoose

Sequence diagram for the `device-choose` listener (consumer group `gard-service-choose`,
`max.poll.records=1`), which waits until the event's `dueAt` (choose wait passed), calls OGrad
`choose`, transitions the device to `activating` and queues the first status check.

```mermaid
sequenceDiagram
    participant Kafka as Kafka (device-choose)
    participant DeviceStepConsumer
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OGradRepository
    participant OGrad API
    participant OutboxRepository

    Kafka->>DeviceStepConsumer: onChoose(message)
    DeviceStepConsumer->>DeviceStepConsumer: parse JSON to DeviceStepEvent(deviceId, imei, expectedState, dueAt)

    alt now < dueAt
        DeviceStepConsumer->>Kafka: nack(min(dueAt - now, 30s))
        Note right of Kafka: container pauses its partitions and redelivers<br/>the same record after the wait, thread is not blocked.<br/>All events share one delay, so only the partition head waits
    else dueAt reached
        DeviceStepConsumer->>DeviceService: handleChoose(event)

        DeviceService->>DeviceRepository: find device by id (pre-check)
        DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
        MariaDB-->>DeviceRepository: device
        DeviceRepository-->>DeviceService: device

        alt device missing or state != 'uploaded'
            Note over DeviceService: stale or duplicate event, skip without calling OGrad
        else state = 'uploaded'
            DeviceService->>OGradRepository: choose imei
            OGradRepository->>OGrad API: choose imei (retried up to 3x)
            OGrad API-->>OGradRepository: ok
            OGradRepository-->>DeviceService: ok

            DeviceService->>DeviceService: transition (TransactionTemplate)
            DeviceService->>DeviceRepository: re-fetch device
            DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
            MariaDB-->>DeviceRepository: device
            DeviceRepository-->>DeviceService: device

            alt state changed while OGrad call was running
                Note over DeviceService: skip, another event already advanced the device
            else choose succeeded
                Note right of DeviceService: markAwaitingStatus: state = 'activating',<br/>statusDeadlineAt = now + 300s
                DeviceService->>OutboxRepository: save DeviceStepEvent(deviceId, imei, 'activating', dueAt = null)
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-status-check)
                MariaDB-->>OutboxRepository: ok
                OutboxRepository-->>DeviceService: ok
                DeviceService->>MariaDB: UPDATE devices SET state = 'activating', ... WHERE id AND version (flush at commit)
                MariaDB-->>DeviceService: ok
            else choose failed after retries
                Note right of DeviceService: markFailed: state = 'failed'
                DeviceService->>OutboxRepository: save REGISTER_FAILED response
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-responses)
                MariaDB-->>OutboxRepository: ok
                OutboxRepository-->>DeviceService: ok
                DeviceService->>MariaDB: UPDATE devices SET state = 'failed', ... WHERE id AND version (flush at commit)
                MariaDB-->>DeviceService: ok
            end
        end

        DeviceService-->>DeviceStepConsumer: ok
        DeviceStepConsumer->>Kafka: acknowledge()
    end
```
