package com.gard;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mariadb.MariaDBContainer;

@SpringBootTest
@AutoConfigureMockMvc
public abstract class AbstractIntegrationTest {

    static final MariaDBContainer mariadb = new MariaDBContainer("mariadb:11");

    protected static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.4.0");

    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static final GenericContainer<?> httpbin = new GenericContainer<>("kennethreitz/httpbin").withExposedPorts(80);

    static {
        mariadb.start();
        kafka.start();
        redis.start();
        httpbin.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mariadb::getJdbcUrl);
        registry.add("spring.datasource.username", mariadb::getUsername);
        registry.add("spring.datasource.password", mariadb::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> String.valueOf(redis.getMappedPort(6379)));
        registry.add("ograd.choose.wait-seconds", () -> "1");
        registry.add("ograd.status.poll-interval-seconds", () -> "1");
        registry.add("ograd.status.max-wait-seconds", () -> "5");
        registry.add("ograd.http.base-url", () -> "http://" + httpbin.getHost() + ":" + httpbin.getMappedPort(80));
        registry.add("ograd.http.delay-seconds", () -> "0");
    }
}