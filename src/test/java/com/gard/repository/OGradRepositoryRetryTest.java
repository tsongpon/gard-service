package com.gard.repository;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OGradRepositoryRetryTest {

    private AnnotationConfigApplicationContext context;

    @AfterEach
    void tearDown() {
        FlakyOGradRepository.validateAttempts.set(0);
        FlakyOGradRepository.uploadAttempts.set(0);
        if (context != null) {
            context.close();
        }
    }

    @Test
    void retriesTransientFailureAndSucceedsOnThirdAttempt() {
        context = new AnnotationConfigApplicationContext(RetryTestConfig.class);
        FlakyOGradRepository repository = context.getBean(FlakyOGradRepository.class);

        repository.validateImei("111");

        assertThat(FlakyOGradRepository.validateAttempts.get()).isEqualTo(3);
    }

    @Test
    void rethrowsAfterMaxAttemptsExhausted() {
        context = new AnnotationConfigApplicationContext(RetryTestConfig.class);
        FlakyOGradRepository repository = context.getBean(FlakyOGradRepository.class);

        assertThatThrownBy(() -> repository.upload("111")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("permanent failure");
        assertThat(FlakyOGradRepository.uploadAttempts.get()).isEqualTo(3);
    }

    @Configuration
    @EnableRetry
    static class RetryTestConfig {

        @Bean
        FlakyOGradRepository flakyOGradRepository() {
            return new FlakyOGradRepository();
        }
    }

    static class FlakyOGradRepository extends OGradRepository {

        static final AtomicInteger validateAttempts = new AtomicInteger();

        static final AtomicInteger uploadAttempts = new AtomicInteger();

        FlakyOGradRepository() {
            super("http://localhost:1", 0);
        }

        @Override
        protected void sendRequest(String operation, String imei) {
        }

        @Override
        @Retryable(retryFor = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 1, multiplier = 2))
        public void validateImei(String imei) {
            if (validateAttempts.incrementAndGet() < 3) {
                throw new IllegalStateException("transient failure");
            }
            super.validateImei(imei);
        }

        @Override
        @Retryable(retryFor = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 1, multiplier = 2))
        public void upload(String imei) {
            uploadAttempts.incrementAndGet();
            throw new IllegalStateException("permanent failure");
        }
    }
}