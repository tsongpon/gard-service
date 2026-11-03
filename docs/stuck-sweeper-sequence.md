# Stuck Device Sweeper — sweepStuckDevices

Sequence diagram for the `sweep` scheduled job (every 60s, ShedLock `device-stuck-sweeper`), the safety
net of the Kafka-driven workflow. It re-queues the current step event for devices that have stayed in a
non-final state longer than `device.sweeper.stuck-seconds` (600s), e.g. because their event went to a
DLT or they were left in flight by the earlier scheduler-based version. Re-queuing is safe because every
step re-checks the device state. Events are re-queued with no delay: `stuck-seconds` must exceed the choose
wait (checked at startup), so any choose wait has already passed.

```mermaid
sequenceDiagram
    participant Scheduler
    participant DeviceSweeperSchedule
    participant DeviceService
    participant DeviceRepository
    participant MariaDB
    participant OutboxRepository

    Scheduler->>DeviceSweeperSchedule: sweep
    Note right of DeviceSweeperSchedule: @Scheduled every 60s, @SchedulerLock (ShedLock/Redis) ensures only one node runs it
    DeviceSweeperSchedule->>DeviceService: sweepStuckDevices
    DeviceService->>DeviceRepository: find stuck devices (cutoff = now - 600s)
    DeviceRepository->>MariaDB: SELECT top 50 devices WHERE state IN ('enrolling', 'uploaded', 'activating') AND coalesce(updated_at, created_at) < cutoff ORDER BY coalesce(updated_at, created_at)
    MariaDB-->>DeviceRepository: stuck devices
    DeviceRepository-->>DeviceService: stuck devices

    loop through all stuck devices
        DeviceService->>DeviceService: transition (TransactionTemplate)
        DeviceService->>DeviceRepository: re-fetch device
        DeviceRepository->>MariaDB: SELECT devices WHERE id = ?
        MariaDB-->>DeviceRepository: device
        DeviceRepository-->>DeviceService: device

        alt state changed or updated since cutoff
            Note over DeviceService: device moved on since the query, skip
        else still stuck
            Note right of DeviceService: touch: updatedAt = now,<br/>so the next sweep does not pick it again right away
            alt state = 'enrolling'
                DeviceService->>OutboxRepository: save DeviceStepEvent(dueAt = null)
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-enroll)
            else state = 'uploaded'
                DeviceService->>OutboxRepository: save DeviceStepEvent(dueAt = null)
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-choose)
            else state = 'activating'
                DeviceService->>OutboxRepository: save DeviceStepEvent(dueAt = null)
                OutboxRepository->>MariaDB: INSERT INTO outbox_messages (topic = device-status-check)
            end
            MariaDB-->>OutboxRepository: ok
            OutboxRepository-->>DeviceService: ok
            DeviceService->>MariaDB: UPDATE devices SET updated_at = now WHERE id AND version (flush at commit)
            MariaDB-->>DeviceService: ok
        end
        Note right of DeviceService: version conflict (OptimisticLockingFailureException)<br/>means a step consumer advanced it concurrently, skip
    end

    DeviceService-->>DeviceSweeperSchedule: ok
    DeviceSweeperSchedule-->>Scheduler: ok
```
