package sku.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sku.model.Order;
import sku.model.OrderOutcome;

import java.util.List;
import java.util.Objects;

/** Commits the order and its final reservation decision in the same MySQL transaction. */
@Service
public class OrderPersistenceService {
    private static final RowMapper<Order> ORDER_MAPPER = (rs, rowNum) -> new Order(
            rs.getLong("id"), rs.getLong("sku_id"), rs.getLong("user_id"));
    private static final RowMapper<StoredOutcome> OUTCOME_MAPPER = (rs, rowNum) -> new StoredOutcome(
            new Order(rs.getLong("order_id"), rs.getLong("sku_id"), rs.getLong("user_id")),
            rs.getString("status"), rs.getBoolean("keep_buyer"));

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OrderPersistenceService(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /** Called through a separate Spring bean so commit finishes before Redis completion. */
    @Transactional
    public OrderOutcome persist(Order order) {
        validate(order);
        jdbc.update("""
                INSERT INTO order_outcome (order_id, sku_id, user_id, status)
                VALUES (?, ?, ?, 'PROCESSING')
                ON DUPLICATE KEY UPDATE order_id = order_id
                """, order.getId(), order.getSkuId(), order.getUserId());
        StoredOutcome existing = jdbc.queryForObject("""
                SELECT order_id, sku_id, user_id, status, keep_buyer
                FROM order_outcome WHERE order_id = ? FOR UPDATE
                """, OUTCOME_MAPPER, order.getId());
        if (existing == null) {
            throw new IllegalStateException("Missing order outcome: " + order.getId());
        }
        requireSameOrder(order, existing.order());
        if (!"PROCESSING".equals(existing.status())) {
            return existing.terminal();
        }

        try {
            requireOne(jdbc.update("""
                    INSERT INTO `order` (id, sku_id, user_id) VALUES (?, ?, ?)
                    """, order.getId(), order.getSkuId(), order.getUserId()), "insert order", order);
        } catch (DuplicateKeyException duplicateOrder) {
            // Only this INSERT may be treated as duplicate consumption. Outbox errors must roll back.
            List<Order> sameId = jdbc.query("""
                    SELECT id, sku_id, user_id FROM `order` WHERE id = ? FOR UPDATE
                    """, ORDER_MAPPER, order.getId());
            if (!sameId.isEmpty()) {
                requireSameOrder(order, sameId.get(0));
                return finish(order, true, true);
            }
            List<Order> previousPurchase = jdbc.query("""
                    SELECT id, sku_id, user_id FROM `order`
                    WHERE sku_id = ? AND user_id = ? FOR UPDATE
                    """, ORDER_MAPPER, order.getSkuId(), order.getUserId());
            if (!previousPurchase.isEmpty()) {
                // Return this reservation's stock, retaining the buyer restriction for the earlier order.
                return finish(order, false, true);
            }
            throw duplicateOrder;
        }

        int stockRows = jdbc.update("""
                UPDATE sku SET stock = stock - 1 WHERE id = ? AND stock > 0
                """, order.getSkuId());
        if (stockRows == 0) {
            requireOne(jdbc.update("DELETE FROM `order` WHERE id = ?", order.getId()),
                    "remove rejected order", order);
            return finish(order, false, false);
        }
        requireOne(stockRows, "decrement stock", order);

        final String payload;
        try {
            payload = objectMapper.writeValueAsString(order);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid order payload", ex);
        }
        requireOne(jdbc.update("""
                INSERT INTO outbox (event_id, payload, status, retry_count, event_type)
                VALUES (?, ?, 0, 0, ?)
                """, order.getId(), payload, Order.class.getSimpleName()), "insert outbox", order);
        return finish(order, true, true);
    }

    public List<OrderOutcome> findPendingRedisOutcomes(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Recovery batch size must be positive");
        }
        return jdbc.query("""
                SELECT order_id, sku_id, user_id, status, keep_buyer
                FROM order_outcome
                WHERE redis_applied = 0 AND next_retry_at <= CURRENT_TIMESTAMP(3)
                    AND status IN ('CONFIRMED', 'CANCELLED')
                ORDER BY next_retry_at, order_id LIMIT ?
                """, OUTCOME_MAPPER, batchSize).stream().map(StoredOutcome::terminal).toList();
    }

    public void markRedisApplied(long orderId) {
        // A concurrent recovery task may have marked it already; Redis completion is idempotent.
        jdbc.update("""
                UPDATE order_outcome SET redis_applied = 1
                WHERE order_id = ? AND status IN ('CONFIRMED', 'CANCELLED') AND redis_applied = 0
                """, orderId);
    }

    public void postponeRedisCompletion(long orderId, int retryDelaySeconds) {
        if (retryDelaySeconds < 1) {
            throw new IllegalArgumentException("Recovery delay must be positive");
        }
        jdbc.update("""
                UPDATE order_outcome
                SET next_retry_at = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))
                WHERE order_id = ? AND redis_applied = 0
                """, retryDelaySeconds, orderId);
    }

    private OrderOutcome finish(Order order, boolean accepted, boolean keepBuyer) {
        requireOne(jdbc.update("""
                UPDATE order_outcome
                SET status = ?, keep_buyer = ?, redis_applied = 0, next_retry_at = CURRENT_TIMESTAMP(3)
                WHERE order_id = ? AND status = 'PROCESSING'
                """, accepted ? "CONFIRMED" : "CANCELLED", keepBuyer, order.getId()),
                "record order outcome", order);
        return new OrderOutcome(order, accepted, keepBuyer);
    }

    private static void validate(Order order) {
        if (order == null || order.getId() == null || order.getId() <= 0
                || order.getSkuId() == null || order.getSkuId() <= 0
                || order.getUserId() == null || order.getUserId() <= 0) {
            throw new IllegalArgumentException("Invalid order message");
        }
    }

    private static void requireSameOrder(Order incoming, Order stored) {
        if (!Objects.equals(incoming.getId(), stored.getId())
                || !Objects.equals(incoming.getSkuId(), stored.getSkuId())
                || !Objects.equals(incoming.getUserId(), stored.getUserId())) {
            throw new IllegalArgumentException("Conflicting payload for order id " + incoming.getId());
        }
    }

    private static void requireOne(int affectedRows, String operation, Order order) {
        if (affectedRows != 1) {
            throw new IllegalStateException("Failed to " + operation + ": " + order.getId());
        }
    }

    private record StoredOutcome(Order order, String status, boolean keepBuyer) {
        private OrderOutcome terminal() {
            if (!"CONFIRMED".equals(status) && !"CANCELLED".equals(status)) {
                throw new IllegalStateException("Nonterminal order outcome: " + status);
            }
            return new OrderOutcome(order, "CONFIRMED".equals(status), keepBuyer);
        }
    }
}
