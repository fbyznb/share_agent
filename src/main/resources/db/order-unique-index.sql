-- Manual migration: review the results before executing the ALTER statement.
-- This script is deliberately excluded from Spring Boot startup initialization.

-- Resolve existing duplicate purchases before adding the index.
SELECT sku_id, user_id, COUNT(*) AS duplicate_count
FROM `order`
GROUP BY sku_id, user_id
HAVING COUNT(*) > 1;

-- Run only if uk_order_sku_user (or an equivalent unique index) does not exist.
-- Existing duplicate data makes this statement fail; do not delete orders to force it through.
-- Inspect with: SHOW INDEX FROM `order`;
ALTER TABLE `order` ADD UNIQUE INDEX uk_order_sku_user (sku_id, user_id);
