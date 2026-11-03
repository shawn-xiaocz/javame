# Javame Matching Service

Java 17 / Spring Boot / gRPC / MySQL 8.4 / RabbitMQ. The Java implementation provides durable asynchronous limit orders, idempotent placement and cancellation, exact decimal arithmetic, price/time priority, order status, two-sided depth, and a transactional event outbox.

```bash
cp .env.example .env
# Replace the local database password, RabbitMQ password and service API key.
docker compose up --build -d
```

HTTP listens on port 8080 and gRPC on 8088. Requests require an `X-Api-Key` header or `x-api-key` gRPC metadata. `ACCEPTED` means the command has been committed, not that the order has filled.

Run `./gradlew test` for unit tests, `./gradlew integrationTest` for isolated MySQL 8.4/RabbitMQ tests using Docker, and `./gradlew check bootJar` to verify and package the service.

- [中文说明](README_cn.md)
