-- Spring Boot executes this idempotent schema script before database access.
-- Existing tables and business records are preserved.

CREATE TABLE IF NOT EXISTS order_outcome (
    order_id BIGINT NOT NULL,
    sku_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL,
    keep_buyer BOOLEAN NOT NULL DEFAULT FALSE,
    redis_applied BOOLEAN NOT NULL DEFAULT FALSE,
    next_retry_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (order_id),
    KEY idx_order_outcome_recovery (redis_applied, next_retry_at, order_id)
) ENGINE=InnoDB;
