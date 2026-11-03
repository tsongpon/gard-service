package com.gard.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "devices")
public class Device {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(unique = true)
    private String imei;

    @Column(name = "app_reference_id", unique = true)
    private String appReferenceId;

    @Column(name = "account_number")
    private String accountNumber;

    private String state;

    private String status;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "next_status_check_at")
    private LocalDateTime nextStatusCheckAt;

    @Column(name = "status_deadline_at")
    private LocalDateTime statusDeadlineAt;

    @Column(name = "choose_not_before")
    private LocalDateTime chooseNotBefore;

    @Version
    private long version;

    protected Device() {
    }

    public Device(String appReferenceId, String imei, String accountNumber, String state, String status,
            LocalDateTime createdAt) {
        this.appReferenceId = appReferenceId;
        this.imei = imei;
        this.accountNumber = accountNumber;
        this.state = state;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = createdAt;
    }

    public void markUploaded(LocalDateTime chooseNotBefore) {
        this.state = "uploaded";
        this.chooseNotBefore = chooseNotBefore;
        this.updatedAt = LocalDateTime.now();
    }

    public void markAwaitingStatus(LocalDateTime nextStatusCheckAt, LocalDateTime statusDeadlineAt) {
        this.state = "activating";
        this.nextStatusCheckAt = nextStatusCheckAt;
        this.statusDeadlineAt = statusDeadlineAt;
        this.updatedAt = LocalDateTime.now();
    }

    public void rescheduleStatusCheck(LocalDateTime nextStatusCheckAt) {
        this.nextStatusCheckAt = nextStatusCheckAt;
        this.updatedAt = LocalDateTime.now();
    }

    public void markEnrolled(String status) {
        this.state = "enrolled";
        this.status = status;
        this.updatedAt = LocalDateTime.now();
    }

    public void touch() {
        this.updatedAt = LocalDateTime.now();
    }

    public void markFailed() {
        this.state = "failed";
        this.updatedAt = LocalDateTime.now();
    }

    public void resetForRetry(String appReferenceId) {
        this.appReferenceId = appReferenceId;
        this.state = "enrolling";
        this.status = "pending";
        this.chooseNotBefore = null;
        this.nextStatusCheckAt = null;
        this.statusDeadlineAt = null;
        this.updatedAt = LocalDateTime.now();
    }

    public String getId() {
        return id;
    }

    public String getImei() {
        return imei;
    }

    public String getAppReferenceId() {
        return appReferenceId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public String getState() {
        return state;
    }

    public String getStatus() {
        return status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public LocalDateTime getNextStatusCheckAt() {
        return nextStatusCheckAt;
    }

    public LocalDateTime getStatusDeadlineAt() {
        return statusDeadlineAt;
    }

    public LocalDateTime getChooseNotBefore() {
        return chooseNotBefore;
    }

    public long getVersion() {
        return version;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}