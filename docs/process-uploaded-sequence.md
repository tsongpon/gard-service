# Choose Worker — processUploaded

Sequence diagram for the `processUploaded` scheduled job (every 5s, ShedLock
`ograd-choose-worker`), which picks `uploaded` devices whose choose wait has passed,
calls OGrad `choose` and transitions them to `activating` with a status-poll schedule.

```mermaid
sequenceDiagram
    participant Scheduler
    participant DeviceRegistrationSchedule
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OGradRepository
    participant OGrad API

    Scheduler->>DeviceRegistrationSchedule: processUploaded
    Note right of DeviceRegistrationSchedule: @Scheduled every 5s, @SchedulerLock (ShedLock/Redis) ensures only one node runs it
    DeviceRegistrationSchedule->>DeviceService: processUploaded
    DeviceService->>DeviceRepository: get claimable uploaded devices (choose wait passed)
    DeviceRepository->>MariaDB: SELECT top 50 devices WHERE state = 'uploaded' AND chooseNotBefore <= now AND lease absent/expired ORDER BY id
    MariaDB-->>DeviceRepository: claimable devices
    DeviceRepository-->>DeviceService: claimable devices

    loop through all uploaded devices
        DeviceService->>DeviceRepository: tryClaim(deviceId, 'uploaded', until = now + lease)
        DeviceRepository->>MariaDB: UPDATE devices SET processing_until = until WHERE id AND state AND lease absent/expired
        MariaDB-->>DeviceRepository: affected rows (1 = claimed, 0 = taken)
        DeviceRepository-->>DeviceService: claimed

        alt claim failed (0 rows)
            Note over DeviceService: another worker owns this device, skip
        else claim succeeded (1 row)
            DeviceService->>OGradRepository: choose imei
            OGradRepository->>OGrad API: choose imei
            OGrad API-->>OGradRepository: ok
            OGradRepository-->>DeviceService: ok
            DeviceService->>DeviceService: completeChoose (transaction)
            Note right of DeviceService: re-fetch and verify state still 'uploaded',<br/>markAwaitingStatus: state = 'activating',<br/>nextStatusCheckAt = now + 10s, statusDeadlineAt = now + 5 min,<br/>processingUntil = null
            DeviceService->>DeviceRepository: save activating device
            DeviceRepository->>MariaDB: UPDATE devices SET state = 'activating', ... (flush at commit)
            MariaDB-->>DeviceRepository: ok
            DeviceRepository-->>DeviceService: ok
        end
    end

    DeviceService-->>DeviceRegistrationSchedule: ok
    DeviceRegistrationSchedule-->>Scheduler: ok
```