package com.gard.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import com.gard.entity.Device;

import org.springframework.data.jpa.repository.JpaRepository;

public interface DeviceRepository extends JpaRepository<Device, String> {

    Optional<Device> findByImei(String imei);

    Optional<Device> findByAppReferenceId(String appReferenceId);

    List<Device> findTop50ByStateOrderByIdAsc(String state);

    List<Device> findTop50ByStateAndChooseNotBeforeLessThanEqualOrderByIdAsc(String state, LocalDateTime time);

    List<Device> findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(String state,
            LocalDateTime time);
}