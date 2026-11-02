# Registration Worker — processEnrolling

Sequence diagram for the `processEnrolling` scheduled job (every 5s, ShedLock
`ograd-registration-worker`), which drives `enrolling` devices through OGrad
`validateImei` + `upload` and transitions them to `uploaded`.

```mermaid
sequenceDiagram
    participant Scheduler
    participant DeviceRegistrationSchedule
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OGradRepository
    participant OGrad API

    Scheduler->>DeviceRegistrationSchedule: processEnrolling
    Note right of DeviceRegistrationSchedule: @Scheduled every 5s, @SchedulerLock (ShedLock/Redis) ensures only one node runs it
    DeviceRegistrationSchedule->>DeviceService: processEnrolling
    DeviceService->>DeviceRepository: get claimable enrolling devices
    DeviceRepository->>MariaDB: SELECT top 50 devices WHERE state = 'enrolling' AND lease absent/expired ORDER BY id
    MariaDB-->>DeviceRepository: claimable devices
    DeviceRepository-->>DeviceService: claimable devices

    loop through all enrolling devices
        DeviceService->>DeviceRepository: tryClaim(deviceId, 'enrolling', until = now + lease)
        DeviceRepository->>MariaDB: UPDATE devices SET processing_until = until WHERE id AND state AND lease absent/expired
        MariaDB-->>DeviceRepository: affected rows (1 = claimed, 0 = taken)
        DeviceRepository-->>DeviceService: claimed

        alt claim failed (0 rows)
            Note over DeviceService: another worker owns this device, skip
        else claim succeeded (1 row)
            DeviceService->>OGradRepository: validate imei
            OGradRepository->>OGrad API: validate imei
            OGrad API-->>OGradRepository: ok
            OGradRepository-->>DeviceService: ok
            DeviceService->>OGradRepository: upload
            OGradRepository->>OGrad API: upload
            OGrad API-->>OGradRepository: ok
            OGradRepository-->>DeviceService: ok
            DeviceService->>DeviceService: completeEnrollment (transaction)
            Note right of DeviceService: re-fetch and verify state still 'enrolling',<br/>markUploaded: state = 'uploaded',<br/>chooseNotBefore = now + 1 min,<br/>processingUntil = null
            DeviceService->>DeviceRepository: save uploaded device
            DeviceRepository->>MariaDB: UPDATE devices SET state = 'uploaded', ... (flush at commit)
            MariaDB-->>DeviceRepository: ok
            DeviceRepository-->>DeviceService: ok
        end
    end

    DeviceService-->>DeviceRegistrationSchedule: ok
    DeviceRegistrationSchedule-->>Scheduler: ok
```