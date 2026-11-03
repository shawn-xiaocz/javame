package io.javame;

import io.javame.infrastructure.config.JavameProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Starts the HTTP/gRPC adapters, durable command worker and outbox publisher. */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(JavameProperties.class)
public class JavameApplication {
    public static void main(String[] args) {
        SpringApplication.run(JavameApplication.class, args);
    }
}
