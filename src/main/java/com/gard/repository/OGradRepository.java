package com.gard.repository;

import java.net.http.HttpClient;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestClient;

@Repository
public class OGradRepository {

    private static final Logger log = LoggerFactory.getLogger(OGradRepository.class);

    private static final int CONNECT_TIMEOUT_SECONDS = 2;

    private static final int READ_TIMEOUT_SECONDS = 5;

    private final RestClient restClient;

    private final int delaySeconds;

    public OGradRepository(@Value("${ograd.http.base-url:https://httpbin.org}") String baseUrl,
            @Value("${ograd.http.delay-seconds:1}") int delaySeconds) {
        this.delaySeconds = delaySeconds;
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECONDS))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(READ_TIMEOUT_SECONDS));
        this.restClient = RestClient.builder().baseUrl(baseUrl).requestFactory(requestFactory).build();
    }

    @Retryable(retryFor = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 500, multiplier = 2))
    public void validateImei(String imei) {
        externalCall("validateImei", imei, "VALID");
    }

    @Retryable(retryFor = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 500, multiplier = 2))
    public void upload(String imei) {
        externalCall("upload", imei, "UPLOADED");
    }

    @Retryable(retryFor = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 500, multiplier = 2))
    public void choose(String imei) {
        externalCall("choose", imei, "CHOSEN");
    }

    @Retryable(retryFor = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 500, multiplier = 2))
    public String getStatus(String imei) {
        externalCall("getStatus", imei, "status=normal");
        return "normal";
    }

    private void externalCall(String operation, String imei, String result) {
        long start = System.nanoTime();
        log.info("OGrad API request: operation={} imei={}", operation, imei);
        sendRequest(operation, imei);
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        log.info("OGrad API response: operation={} imei={} result={} ({} ms)", operation, imei, result, elapsedMs);
    }

    protected void sendRequest(String operation, String imei) {
        restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/delay/" + delaySeconds)
                        .queryParam("operation", operation)
                        .queryParam("imei", imei)
                        .build())
                .retrieve()
                .toBodilessEntity();
    }
}