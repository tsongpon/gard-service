# Status Check Step Consumer — onStatusCheck

Sequence diagram for the `device-status-check` listener (consumer group `gard-service-status-check`,
`max.poll.records=1`), which waits until the event's `dueAt`, polls OGrad status for an `activating`
device and either enrolls it, fails it after the deadline, or queues the next check.

```mermaid
sequenceDiagram
    participant Kafka as Kafka (device-status-check)
    participant DeviceStepConsumer
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OGradRepository
    participant OGrad API
    participant OutboxRepository

    Kafka->>DeviceStepConsumer: onStatusCheck(message)
    DeviceStepConsumer->>DeviceStepConsumer: parse JSON to DeviceStepEvent(deviceId, imei, expectedState, dueAt)

    alt now < dueAt
        DeviceStepConsumer->>Kafka: nack(min(dueAt - now, 30s))
        Note right of Kafka: partitions paused, same record redelivered after the wait
    else dueAt reached
        DeviceStepConsumer->>DeviceService: handleStatusCheck(event)

        DeviceService->>DeviceRepository: find device by id (pre-check)
        DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
        MariaDB-->>DeviceRepository: device
        DeviceRepository-->>DeviceService: device

        alt device missing or state != 'activating'
            Note over DeviceService: stale or duplicate event, skip without calling OGrad
        else state = 'activating'
            DeviceService->>OGradRepository: get status imei
            OGradRepository->>OGrad API: get status imei (retried up to 3x)
            OGrad API-->>OGradRepository: status
            OGradRepository-->>DeviceService: status (null if the call failed)

            DeviceService->>DeviceService: transition (TransactionTemplate)
            DeviceService->>DeviceRepository: re-fetch device
            DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
            MariaDB-->>DeviceRepository: device
            DeviceRepository-->>DeviceService: device

            alt state changed while OGrad call was running
                Note over DeviceService: skip, another event already advanced the device
            else status = 'normal'
                Note right of DeviceService: markEnrolled: state = 'enrolled', status = 'normal'
                DeviceService->>OutboxRepository: save REGISTERED response
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-responses)
                MariaDB-->>OutboxRepository: ok
                OutboxRepository-->>DeviceService: ok
                DeviceService->>MariaDB: UPDATE devices SET state = 'enrolled', ... WHERE id AND version (flush at commit)
                MariaDB-->>DeviceService: ok
            else status != 'normal' and now > statusDeadlineAt
                Note right of DeviceService: markFailed: state = 'failed'
                DeviceService->>OutboxRepository: save REGISTER_FAILED response (status not normal within 300s)
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-responses)
                MariaDB-->>OutboxRepository: ok
                OutboxRepository-->>DeviceService: ok
                DeviceService->>MariaDB: UPDATE devices SET state = 'failed', ... WHERE id AND version (flush at commit)
                MariaDB-->>DeviceService: ok
            else status != 'normal' and now <= statusDeadlineAt
                Note right of DeviceService: touch: updatedAt = now,<br/>state stays 'activating'
                DeviceService->>OutboxRepository: save DeviceStepEvent(deviceId, imei, 'activating', dueAt = now + 10s)
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-status-check)
                MariaDB-->>OutboxRepository: ok
                OutboxRepository-->>DeviceService: ok
                DeviceService->>MariaDB: UPDATE devices SET updated_at = now WHERE id AND version (flush at commit)
                MariaDB-->>DeviceService: ok
            end
        end

        DeviceService-->>DeviceStepConsumer: ok
        DeviceStepConsumer->>Kafka: acknowledge()
    end
```
