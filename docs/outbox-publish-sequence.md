# Outbox Publisher — publishPendingMessages

Sequence diagram for the `publishPending` scheduled job (every 1s, ShedLock `outbox-publisher`), which
sends outbox rows (step events and responses) to Kafka in id order. It stops the batch at the first
failed send so a device's events are never published out of order.

```mermaid
sequenceDiagram
    participant Scheduler
    participant OutboxPublishSchedule
    participant OutboxService
    participant OutboxRepository
    participant MariaDB
    participant KafkaTemplate
    participant Kafka

    Scheduler->>OutboxPublishSchedule: publishPending
    Note right of OutboxPublishSchedule: @Scheduled every 1s, @SchedulerLock (ShedLock/Redis) ensures only one node runs it
    OutboxPublishSchedule->>OutboxService: publishPendingMessages
    OutboxService->>OutboxRepository: get pending messages
    OutboxRepository->>MariaDB: SELECT top 50 outbox_messages ORDER BY id
    MariaDB-->>OutboxRepository: pending messages
    OutboxRepository-->>OutboxService: pending messages

    loop through pending messages in id order
        OutboxService->>KafkaTemplate: send(topic, key = deviceId, payload)
        KafkaTemplate->>Kafka: produce record
        Kafka-->>KafkaTemplate: ack / error
        KafkaTemplate-->>OutboxService: result (wait up to 10s)

        alt sent
            OutboxService->>OutboxRepository: delete message
            OutboxRepository->>MariaDB: DELETE FROM outbox_messages WHERE id = ?
            MariaDB-->>OutboxRepository: ok
            OutboxRepository-->>OutboxService: ok
        else send failed or timed out
            Note over OutboxService: stop the batch, message and all later ones<br/>stay in the outbox and are retried next cycle
        else interrupted
            Note over OutboxService: restore interrupt flag and abort without deleting
        end
    end
    Note right of OutboxService: a crash between send and delete republishes the message,<br/>delivery is at-least-once, consumers re-check device state

    OutboxService-->>OutboxPublishSchedule: ok
    OutboxPublishSchedule-->>Scheduler: ok
```
