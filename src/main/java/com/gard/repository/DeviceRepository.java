package com.gard.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import com.gard.entity.Device;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface DeviceRepository extends JpaRepository<Device, String> {

    Optional<Device> findByImei(String imei);

    Optional<Device> findByAppReferenceId(String appReferenceId);

    List<Device> findTop50ByStateAndNextStatusCheckAtLessThanEqualOrderByIdAsc(String state,
            LocalDateTime time);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            update Device d
               set d.processingUntil = :until
             where d.id = :id
               and d.state = :state
               and (d.processingUntil is null or d.processingUntil < :now)
            """)
    int tryClaim(@Param("id") String id, @Param("state") String state, @Param("until") LocalDateTime until,
            @Param("now") LocalDateTime now);

    @Query("""
            select d from Device d
             where d.state = :state
               and (d.processingUntil is null or d.processingUntil < :now)
             order by d.id asc
            """)
    List<Device> findClaimableByState(@Param("state") String state, @Param("now") LocalDateTime now,
            Pageable pageable);

    @Query("""
            select d from Device d
             where d.state = :state
               and d.chooseNotBefore <= :time
               and (d.processingUntil is null or d.processingUntil < :now)
             order by d.id asc
            """)
    List<Device> findClaimableForChoose(@Param("state") String state, @Param("time") LocalDateTime time,
            @Param("now") LocalDateTime now, Pageable pageable);
}