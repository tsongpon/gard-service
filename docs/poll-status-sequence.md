# Status Poller Worker — pollStatus

Sequence diagram for the `pollStatus` scheduled job (every 5s, ShedLock
`ograd-status-poller`), which polls `activating` devices whose status check is due
and transitions them to `enrolled` once OGrad reports status `normal`.
No per-device lease is claimed here: `getStatus` is read-only, so a duplicate poll is harmless.

```mermaid
sequenceDiagram
    participant Scheduler
    participant DeviceRegistrationSchedule
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OGradRepository
    participant OGrad API
    participant OutboxRepository

    Scheduler->>DeviceRegistrationSchedule: pollStatus
    Note right of DeviceRegistrationSchedule: @Scheduled every 5s, @SchedulerLock (ShedLock/Redis) ensures only one node runs it
    DeviceRegistrationSchedule->>DeviceService: pollStatus
    DeviceService->>DeviceRepository: get due activating devices
    DeviceRepository->>MariaDB: SELECT top 50 devices WHERE state = 'activating' AND next_status_check_at <= now ORDER BY id
    MariaDB-->>DeviceRepository: due devices
    DeviceRepository-->>DeviceService: due devices

    loop through all due devices
        DeviceService->>OGradRepository: get status imei
        OGradRepository->>OGrad API: get status imei
        OGrad API-->>OGradRepository: status
        OGradRepository-->>DeviceService: status
        DeviceService->>DeviceService: completeStatusPoll (transaction)
        Note right of DeviceService: re-fetch and verify state still 'activating',<br/>skip if another worker already transitioned it

        alt status = 'normal'
            DeviceService->>DeviceService: markEnrolled
            Note right of DeviceService: set state = 'enrolled', status = 'normal'
            DeviceService->>DeviceRepository: save enrolled device
            DeviceRepository->>MariaDB: UPDATE devices SET state = 'enrolled', ... (flush at commit)
            MariaDB-->>DeviceRepository: ok
            DeviceRepository-->>DeviceService: ok
            DeviceService->>OutboxRepository: save REGISTERED response
            OutboxRepository->>MariaDB: INSERT INTO outbox_messages (device-responses, same transaction)
            MariaDB-->>OutboxRepository: ok
            OutboxRepository-->>DeviceService: ok
        else status != 'normal' and now > statusDeadlineAt
            DeviceService->>DeviceService: markFailed
            Note right of DeviceService: set state = 'failed'
            DeviceService->>DeviceRepository: save failed device
            DeviceRepository->>MariaDB: UPDATE devices SET state = 'failed', ... (flush at commit)
            MariaDB-->>DeviceRepository: ok
            DeviceRepository-->>DeviceService: ok
            DeviceService->>OutboxRepository: save REGISTER_FAILED response (status not normal within 300s)
            OutboxRepository->>MariaDB: INSERT INTO outbox_messages (device-responses, same transaction)
            MariaDB-->>OutboxRepository: ok
            OutboxRepository-->>DeviceService: ok
        else status != 'normal' and now <= statusDeadlineAt
            DeviceService->>DeviceService: rescheduleStatusCheck
            Note right of DeviceService: set next_status_check_at = now + 10s,<br/>state stays 'activating', no outbox event
            DeviceService->>DeviceRepository: save rescheduled device
            DeviceRepository->>MariaDB: UPDATE devices SET next_status_check_at = now + 10s (flush at commit)
            MariaDB-->>DeviceRepository: ok
            DeviceRepository-->>DeviceService: ok
        end
    end

    DeviceService-->>DeviceRegistrationSchedule: ok
    DeviceRegistrationSchedule-->>Scheduler: ok
```