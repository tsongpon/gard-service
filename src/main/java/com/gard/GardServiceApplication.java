package com.gard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableRetry
public class GardServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(GardServiceApplication.class, args);
	}

}