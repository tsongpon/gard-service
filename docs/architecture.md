# gard-service — Runtime Flow Diagrams

gard-service consumes device-register commands from Kafka `device-commands`, drives the device
through the OGrad registration flow (validate IMEI, upload, wait, choose, wait for status), and
publishes exactly one response event (`REGISTERED`, `REGISTER_FAILED`, or `REGISTER_REJECTED`)
to `device-responses` via a durable outbox. All flow progress is stored in the `devices` table,
so every step survives crashes and resumes on any node.

## Device lifecycle

```mermaid
stateDiagram-v2
    [*] --> enrolling: register command accepted
    enrolling --> uploaded: validateImei + upload ok
    enrolling --> failed: step failed after retries
    uploaded --> provisioning: choose ok (after choose wait)
    uploaded --> failed: choose failed after retries
    provisioning --> enrolled: getStatus normal
    provisioning --> failed: status deadline exceeded
    failed --> enrolling: resend with new appReferenceId (resetForRetry)
    enrolled --> [*]
```

## 1. Register command received (`device-commands`)

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka (device-commands)
    participant C as DeviceCommandConsumer
    participant S as DeviceService register TX
    participant R as DeviceRepository
    participant O as OutboxRepository

    K->>C: consumeRegisterDeviceCommand(message)
    C->>C: parse JSON to DeviceRegisterRequest(imei, appReferenceId)
    C->>S: register(request)
    alt appReferenceId null or blank
        S->>O: save(REGISTER_REJECTED appReferenceId is required)
    else appReferenceId already in devices
        S-->>S: replay - log warn, skip
    else imei already in devices
        alt existing state == failed
            S->>R: resetForRetry(new appReferenceId)
            Note right of R: state=enrolling status=pending<br/>chooseNotBefore and status schedule cleared
        else existing state enrolling uploaded provisioning enrolled
            S->>O: save(REGISTER_REJECTED imei already registered)
        end
    else fresh imei and reference
        S->>R: save(new Device state=enrolling status=pending)
        Note right of S: on unique index race DataIntegrityViolation<br/>re-fetch then same replay or retry-reject logic
    end
    S-->>C: return
    Note over C,O: outbox rows reach device-responses later<br/>via OutboxPublishSchedule
```

## 2. OutboxPublishSchedule (every 1s)

```mermaid
sequenceDiagram
    autonumber
    participant T as Scheduler thread
    participant L as ShedLock (Redis)
    participant E as OutboxPublishSchedule
    participant S as OutboxService
    participant O as OutboxMessageRepository
    participant KT as KafkaTemplate
    participant K as Kafka

    T->>E: tick fixedDelay=1000ms
    E->>L: acquire outbox-publisher (NX, TTL 10m)
    alt lock not acquired
        E-->>T: skipped silently
    else lock acquired
        E->>S: publishPendingMessages()
        S->>O: findTop50ByOrderByIdAsc()
        O-->>S: up to 50 pending rows
        loop each message
            S->>KT: send(topic, key, payload) then get(10s)
            KT->>K: produce
            alt send succeeded
                S->>O: delete(message) - commits in its own TX immediately
            else ExecutionException or Timeout
                S-->>S: log warn, keep row, continue next message
            else InterruptedException
                S-->>T: abort batch, reinterrupt, lock freed
            else other RuntimeException
                S-->>T: escapes - scheduler logs error<br/>rest of batch waits for next cycle
            end
        end
        S-->>E: log Published N successes
        E->>L: release (lockAtLeastFor=1s honored)
    end
```

## Outbox message lifecycle — creation to delivery

Every path that produces a device-response writes its outbox row **in the same transaction** as the
device state change (atomic: the row exists if and only if the state change committed). A single
ShedLock-guarded publisher drains all rows to `device-responses`.

```mermaid
sequenceDiagram
    autonumber
    participant REG as register reject paths
    participant ENR as completeEnrollment
    participant CHO as completeChoose
    participant POL as completeStatusPoll
    participant O as outbox_messages table
    participant P as OutboxPublishSchedule 1s ShedLock
    participant K as Kafka device-responses

    Note over REG,POL: outbox row written in the SAME transaction<br/>as the device state change

    REG->>O: save(REGISTER_REJECTED - appReferenceId is required)
    REG->>O: save(REGISTER_REJECTED - imei already registered)
    ENR->>O: save(REGISTER_FAILED - validate or upload failed)
    CHO->>O: save(REGISTER_FAILED - choose failed)
    POL->>O: save(REGISTERED - status normal, enrolled)
    POL->>O: save(REGISTER_FAILED - status deadline exceeded)

    loop every 1s, up to 50 rows by id ascending
        P->>O: findTop50ByOrderByIdAsc
        O-->>P: pending rows
        P->>K: send(topic, key=deviceId, payload)
        P->>O: delete row - own TX, commits immediately
    end

    Note over P,K: failed send keeps the row and retries next cycle<br/>delivery is at-least-once - consumers dedupe by appReferenceId
```

## 3. processEnrolling (every 5s) — validate + upload

```mermaid
sequenceDiagram
    autonumber
    participant T as Scheduler thread
    participant L as ShedLock (Redis)
    participant D as DeviceRegistrationSchedule
    participant S as DeviceService
    participant R as DeviceRepository
    participant O as OGradRepository (httpbin)
    participant O2 as OutboxRepository

    T->>D: tick fixedDelay=5000ms
    D->>L: acquire ograd-registration-worker (TTL 15m)
    alt lock not acquired
        D-->>T: skipped
    else lock acquired
        D->>S: processEnrolling()
        S->>R: findTop50ByStateOrderByIdAsc(enrolling) - no TX
        R-->>S: detached devices
        loop each device
            S->>O: validateImei(imei) HTTP retry 3x backoff 500ms
            S->>O: upload(imei) HTTP retry 3x
            Note right of O: both calls outside any transaction
            opt a step failed
                Note over S: failure reason captured, HTTP stops
            end
            S->>S: transactionTemplate - short per-device TX
            S->>R: findById(deviceId) re-fetch managed
            alt state is not enrolling
                S-->>S: guard skip
            else step failure
                S->>S: markFailed
                S->>O2: save(REGISTER_FAILED OGrad registration step failed)
            else steps ok
                S->>S: markUploaded(now + chooseWait 60s)
                Note right of R: state=uploaded chooseNotBefore set<br/>version incremented
            end
            opt OptimisticLockingFailure
                S-->>S: caught - warn, continue next device
            end
        end
        D->>L: release
    end
```

## 4. processUploaded (every 5s) — choose, after the 60s wait

```mermaid
sequenceDiagram
    autonumber
    participant T as Scheduler thread
    participant L as ShedLock (Redis)
    participant D as DeviceRegistrationSchedule
    participant S as DeviceService
    participant R as DeviceRepository
    participant O as OGradRepository (httpbin)
    participant O2 as OutboxRepository

    T->>D: tick fixedDelay=5000ms
    D->>L: acquire ograd-choose-worker (TTL 15m)
    alt lock not acquired
        D-->>T: skipped
    else lock acquired
        D->>S: processUploaded()
        S->>R: findTop50ByStateAndChooseNotBeforeLessThanEqual(uploaded, now)
        R-->>S: devices whose chooseNotBefore has passed
        loop each device
            Note over S: at least ograd.choose.wait-seconds=60s since upload
            S->>O: choose(imei) HTTP retry 3x
            S->>S: transactionTemplate - short per-device TX
            S->>R: findById(deviceId) re-fetch managed
            alt state is not uploaded
                S-->>S: guard skip
            else choose failed
                S->>S: markFailed
                S->>O2: save(REGISTER_FAILED OGrad choose step failed)
            else choose ok
                S->>S: markAwaitingStatus(now, now + maxWait 300s)
                Note right of R: state=provisioning<br/>nextStatusCheckAt=now - first status poll immediate
            end
            opt OptimisticLockingFailure
                S-->>S: caught - warn, continue next device
            end
        end
        D->>L: release
    end
```

## 5. pollStatus (every 5s) — getStatus until normal

```mermaid
sequenceDiagram
    autonumber
    participant T as Scheduler thread
    participant L as ShedLock (Redis)
    participant D as DeviceRegistrationSchedule
    participant S as DeviceService
    participant R as DeviceRepository
    participant O as OGradRepository (httpbin)
    participant O2 as OutboxRepository

    T->>D: tick fixedDelay=5000ms
    D->>L: acquire ograd-status-poller (TTL 10m)
    alt lock not acquired
        D-->>T: skipped
    else lock acquired
        D->>S: pollStatus()
        S->>R: findTop50ByStateAndNextStatusCheckAtLessThanEqual(provisioning, now)
        R-->>S: due devices
        loop each device
            S->>O: getStatus(imei) HTTP retry 3x
            opt call failed
                Note over S: status = null
            end
            S->>S: transactionTemplate - short per-device TX
            S->>R: findById(deviceId) re-fetch managed
            alt state is not provisioning
                S-->>S: guard skip
            else status == normal
                S->>S: markEnrolled(normal)
                S->>O2: save(REGISTERED state=enrolled)
            else deadline passed
                S->>S: markFailed
                S->>O2: save(REGISTER_FAILED last status X or unavailable)
            else non-normal before deadline
                S->>S: reschedule nextStatusCheckAt += pollInterval 10s
            end
            opt OptimisticLockingFailure
                S-->>S: caught - warn, continue next device
            end
        end
        D->>L: release
    end
```