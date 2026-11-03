package com.gard.repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.gard.entity.Device;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeviceRepository extends JpaRepository<Device, String> {

    Optional<Device> findByImei(String imei);

    Optional<Device> findByAppReferenceId(String appReferenceId);

    /**
     * Devices in a non-final state that have not changed since {@code cutoff}, oldest first. Falls back to
     * createdAt for rows written before updatedAt was always set.
     */
    @Query("""
            select d from Device d
             where d.state in :states
               and coalesce(d.updatedAt, d.createdAt) < :cutoff
             order by coalesce(d.updatedAt, d.createdAt) asc
            """)
    List<Device> findStuck(@Param("states") Collection<String> states, @Param("cutoff") LocalDateTime cutoff,
            Pageable pageable);
}
