-- MySQL 8.4 / InnoDB. Binary NO PAD collation preserves exact order IDs, including case.
CREATE TABLE market (
    symbol VARCHAR(64) PRIMARY KEY,
    price_scale INTEGER NOT NULL CHECK (price_scale BETWEEN 0 AND 18),
    volume_scale INTEGER NOT NULL CHECK (volume_scale BETWEEN 0 AND 18),
    command_sequence BIGINT NOT NULL DEFAULT 0,
    last_processed_at DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:00.000000'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE matching_order (
    order_id VARCHAR(128) PRIMARY KEY,
    user_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DECIMAL(38,18) NOT NULL CHECK (price > 0),
    quantity DECIMAL(38,18) NOT NULL CHECK (quantity > 0),
    remaining DECIMAL(38,18) NOT NULL,
    status VARCHAR(24) NOT NULL CHECK (status IN ('PENDING','OPEN','PARTIALLY_FILLED','FILLED','CANCELLED')),
    priority BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT remaining_quantity CHECK (remaining >= 0 AND remaining <= quantity),
    INDEX order_book (symbol, side, status, price, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE matching_command (
    command_id VARCHAR(160) PRIMARY KEY,
    symbol VARCHAR(64) NOT NULL,
    market_sequence BIGINT NOT NULL,
    kind VARCHAR(8) NOT NULL CHECK (kind IN ('PLACE','CANCEL')),
    order_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DONE','REJECTED')),
    error_code VARCHAR(64) NOT NULL DEFAULT '',
    message VARCHAR(256) NOT NULL DEFAULT '',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at DATETIME(6),
    UNIQUE KEY market_command_sequence (symbol, market_sequence),
    INDEX pending_command (symbol, status, market_sequence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE trade (
    trade_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    symbol VARCHAR(64) NOT NULL,
    maker_order_id VARCHAR(128) NOT NULL,
    taker_order_id VARCHAR(128) NOT NULL,
    price DECIMAL(38,18) NOT NULL CHECK (price > 0),
    quantity DECIMAL(38,18) NOT NULL CHECK (quantity > 0),
    traded_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE event_outbox (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    symbol VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at DATETIME(6),
    INDEX unpublished_event (symbol, published_at, id),
    INDEX available_event (published_at, available_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
