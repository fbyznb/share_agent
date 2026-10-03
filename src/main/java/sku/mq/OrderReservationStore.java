package sku.mq;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import sku.model.Order;

/**
 * Stores the inventory reservation and its delivery intent together in Redis.
 * The existing stock/set keys and the shared stream require a single Redis shard.
 */
@Component
public class OrderReservationStore {

    public static final String STREAM_KEY = "sku:orders:stream";

    private static final DefaultRedisScript<String> RESERVE = script("reserve-order.lua", String.class);
    private static final DefaultRedisScript<Long> COMPLETE = script("complete-order.lua", Long.class);

    private final StringRedisTemplate redis;

    @Value("${sku.order-stream.max-backlog:100000}")
    private int maxBacklog = 100000;

    public OrderReservationStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Returns the original order ID on a retry, or null when admission is rejected. */
    public Long reserve(Order order) {
        validate(order);
        if (maxBacklog <= 0) {
            throw new IllegalStateException("sku.order-stream.max-backlog must be positive");
        }
        String result = redis.execute(
                RESERVE,
                keys(order, true),
                order.getId().toString(), order.getSkuId().toString(), order.getUserId().toString(),
                Integer.toString(maxBacklog)
        );
        // Lua returns an empty string for an intentional rejection. A null response is
        // ambiguous (e.g. a misconfigured pipeline), and must not masquerade as rejection.
        if (result == null) {
            throw new IllegalStateException("No Redis reservation result returned");
        }
        return result.isEmpty() ? null : Long.valueOf(result);
    }

    /**
     * Applies a durable database outcome. Cancellation restores inventory only once.
     * keepBuyer preserves the legacy purchase marker when another order already exists.
     */
    public void complete(Order order, boolean accepted, boolean keepBuyer) {
        validate(order);
        Long result = redis.execute(
                COMPLETE,
                keys(order, false),
                order.getId().toString(), order.getSkuId().toString(), order.getUserId().toString(),
                accepted ? "1" : "0", keepBuyer ? "1" : "0"
        );
        if (result == null || result != 1L) {
            throw new IllegalStateException("Redis reservation outcome was not applied: " + order.getId());
        }
    }

    private static List<String> keys(Order order, boolean withStream) {
        String stock = "stole:" + order.getSkuId();
        String buyers = "set:" + order.getSkuId();
        String buyer = "sku:order:buyer:" + order.getSkuId() + ":" + order.getUserId();
        String reservation = "sku:order:reservation:" + order.getId();
        return withStream ? List.of(stock, buyers, buyer, reservation, STREAM_KEY)
                : List.of(stock, buyers, buyer, reservation);
    }

    private static void validate(Order order) {
        if (order == null || order.getId() == null || order.getId() <= 0
                || order.getSkuId() == null || order.getSkuId() <= 0
                || order.getUserId() == null || order.getUserId() <= 0) {
            throw new IllegalArgumentException("Order, SKU and user IDs must be positive");
        }
    }

    private static <T> DefaultRedisScript<T> script(String file, Class<T> resultType) {
        DefaultRedisScript<T> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("sku/" + file));
        script.setResultType(resultType);
        return script;
    }
}
