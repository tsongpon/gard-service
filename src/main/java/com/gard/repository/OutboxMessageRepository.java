package com.gard.repository;

import java.util.List;

import com.gard.entity.OutboxMessage;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    List<OutboxMessage> findTop50ByOrderByIdAsc();
}