# Javame 撮合服务

Java 重写版使用 Java 17、Spring Boot、gRPC、MySQL 8.4 和 RabbitMQ。实现下单、撤单、订单状态和双侧深度查询，并修复订单幂等、浮点计算、订单簿原子更新、消息持久化、失败重试和连接泄漏问题。

```bash
cp .env.example .env
# 修改 .env 的数据库密码、RabbitMQ 密码和服务 API key
docker compose up --build -d
```

HTTP 默认端口 8080，gRPC 默认端口 8088。业务请求需要 `X-Api-Key` / `x-api-key`。

```bash
curl -X POST http://localhost:8080/v1/orders \
  -H 'Content-Type: application/json' \
  -H "X-Api-Key: $JAVAME_API_KEY" \
  -d '{"uuid":"user-1","oid":"order-1","symbol":"btc2usdt","transaction":"BUY","price":"100","volume":"0.1"}'
```

上面的环境变量需要先在终端导出；Compose 读取 `.env` 不会自动导出到终端。返回 `ACCEPTED` 代表命令已持久化，最终状态通过订单和命令接口查询。

- [Java 架构、完整 API、可靠性与迁移说明](docs/java-rewrite.md)
- `./gradlew test`：运行不依赖中间件的单元测试。
- `./gradlew integrationTest`：使用 Docker 自动创建隔离数据库和消息队列，运行真实集成测试。
- `./gradlew check bootJar`：验证并构建可执行 jar。

MySQL 使用 8.4 LTS 和 InnoDB，默认库名、业务账号均为 `javame`，端口为 3306。复制 `.env.example` 后需要设置 `MYSQL_ROOT_PASSWORD`、`DATABASE_PASSWORD`、`RABBITMQ_PASSWORD` 和 `JAVAME_API_KEY`。

数据库写事务使用 `READ COMMITTED` 和行锁保持命令顺序；深度查询使用 `REPEATABLE READ` 保证双侧快照一致。价格、数量保存为 `DECIMAL(38,18)`，订单 ID 区分大小写及末尾空格。数据库连接使用 UTC，会话时间与 `DATETIME(6)` 审计字段一致。Flyway 在新 MySQL 数据库中自动创建表结构。

此次切换不自动搬迁已有数据库数据。若已有旧库订单和成交记录，需要停写、对账后迁移到 MySQL；Compose 使用独立的 `mysql-data` 数据卷。

默认固定使用 `mysql:8.4.8`。可通过 `./gradlew integrationTest -PmysqlImage=mysql:8.0` 验证 MySQL 8.0 兼容性；该参数仅选择隔离测试容器，不修改运行配置。
