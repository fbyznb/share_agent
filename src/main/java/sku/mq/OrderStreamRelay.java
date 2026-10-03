package sku.mq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import sku.model.Order;

/**
 * Dedicated consumer group: transfer an order to MQ before acknowledging it in
 * Redis. A crash between send and ACK deliberately causes a duplicate delivery.
 */
@Component
public class OrderStreamRelay implements SmartLifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderStreamRelay.class);
    static final String GROUP = "order-rocketmq-relay";
    static final String TOPIC = "order";
    // This stream has exactly one logical group. Delete only after MQ confirms;
    // ACK and deletion in one script avoid leaking acknowledged entries when a
    // process dies, without MAXLEN trimming unprocessed orders.
    static final DefaultRedisScript<Long> ACK_AND_DELETE = new DefaultRedisScript<>("""
            local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            redis.call('XDEL', KEYS[1], ARGV[2])
            return acknowledged
            """, Long.class);

    private final StringRedisTemplate redis;
    private final StreamOperations<String, String, String> streams;
    private final RocketMQTemplate rocketMQ;
    private final Consumer consumer = Consumer.from(GROUP, UUID.randomUUID().toString());
    private final boolean enabled;
    private final int batchSize;
    private final long pollMillis;
    private final Duration claimIdle;
    private final long retryMillis;
    private final long sendTimeoutMillis;
    private volatile boolean running;
    private Thread worker;
    private String pendingCursor;

    public OrderStreamRelay(
            StringRedisTemplate redis,
            RocketMQTemplate rocketMQ,
            @Value("${sku.order-stream.enabled:true}") boolean enabled,
            @Value("${sku.order-stream.batch-size:100}") int batchSize,
            @Value("${sku.order-stream.poll-millis:1000}") long pollMillis,
            @Value("${sku.order-stream.claim-idle-millis:60000}") long claimIdleMillis,
            @Value("${sku.order-stream.retry-millis:2000}") long retryMillis,
            @Value("${sku.order-stream.send-timeout-millis:3000}") long sendTimeoutMillis
    ) {
        if (batchSize <= 0 || pollMillis <= 0 || retryMillis <= 0
                || sendTimeoutMillis <= 0 || claimIdleMillis <= sendTimeoutMillis) {
            throw new IllegalArgumentException("Invalid order stream relay limits");
        }
        this.redis = redis;
        this.streams = redis.opsForStream();
        this.rocketMQ = rocketMQ;
        this.enabled = enabled;
        this.batchSize = batchSize;
        this.pollMillis = pollMillis;
        this.claimIdle = Duration.ofMillis(claimIdleMillis);
        this.retryMillis = retryMillis;
        this.sendTimeoutMillis = sendTimeoutMillis;
    }

    @Override
    public synchronized void start() {
        if (!enabled || running || (worker != null && worker.isAlive())) {
            return;
        }
        running = true;
        worker = new Thread(this::run, "order-stream-relay");
        worker.setDaemon(true);
        worker.start();
    }

    private void run() {
        boolean groupReady = false;
        try {
            while (running) {
                try {
                    if (!groupReady) {
                        initializeGroup();
                        groupReady = true;
                    }
                    relayNextBatch();
                } catch (Exception ex) {
                    if (!running) {
                        break;
                    }
                    // Recreate after Redis restart only if absent; BUSYGROUP
                    // preserves the existing group's delivery position.
                    groupReady = false;
                    LOGGER.warn("Order stream relay failed; pending orders will be retried", ex);
                    try {
                        Thread.sleep(retryMillis);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            running = false;
        }
    }

    void initializeGroup() {
        try {
            redis.execute((RedisCallback<String>) connection -> connection.streamCommands()
                    .xGroupCreate(OrderReservationStore.STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                            GROUP, ReadOffset.from("0-0"), true));
        } catch (RuntimeException ex) {
            for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
                if (cause.getMessage() != null && cause.getMessage().contains("BUSYGROUP")) {
                    return;
                }
            }
            throw ex;
        }
    }

    void relayNextBatch() {
        reclaimPending();
        List<MapRecord<String, String, String>> records = streams.read(consumer,
                StreamReadOptions.empty().count(batchSize).block(Duration.ofMillis(pollMillis)),
                StreamOffset.create(OrderReservationStore.STREAM_KEY, ReadOffset.lastConsumed()));
        forward(records);
    }

    private void reclaimPending() {
        Range<String> range = pendingCursor == null ? Range.unbounded()
                : Range.of(Range.Bound.exclusive(pendingCursor), Range.Bound.unbounded());
        PendingMessages pending = streams.pending(OrderReservationStore.STREAM_KEY, GROUP,
                range, batchSize);
        if (pending == null || pending.isEmpty()) {
            pendingCursor = null;
            return;
        }
        // Advance even for poison records, so they cannot starve later entries.
        pendingCursor = pending.size() < batchSize ? null
                : pending.get(pending.size() - 1).getIdAsString();
        RecordId[] ids = pending.stream()
                .filter(message -> message.getElapsedTimeSinceLastDelivery().compareTo(claimIdle) >= 0)
                .map(PendingMessage::getId).toArray(RecordId[]::new);
        if (ids.length > 0) {
            forward(streams.claim(OrderReservationStore.STREAM_KEY, GROUP, consumer.getName(), claimIdle, ids));
        }
    }

    private void forward(List<MapRecord<String, String, String>> records) {
        if (records == null) {
            return;
        }
        for (MapRecord<String, String, String> record : records) {
            Order order;
            try {
                order = toOrder(record);
            } catch (IllegalArgumentException ex) {
                // Keep malformed records pending for repair; never silently ACK.
                LOGGER.error("Invalid order stream record {}; retained for repair", record.getId(), ex);
                continue;
            }
            // Transport/ACK failures escape and back off instead of hammering MQ.
            sendAndAcknowledge(record, order);
        }
    }

    void forwardOne(MapRecord<String, String, String> record) {
        sendAndAcknowledge(record, toOrder(record));
    }

    private static Order toOrder(MapRecord<String, String, String> record) {
        Map<String, String> values = record.getValue();
        return new Order(positiveId(values, "orderId"), positiveId(values, "skuId"),
                positiveId(values, "userId"));
    }

    private void sendAndAcknowledge(MapRecord<String, String, String> record, Order order) {
        SendResult result = rocketMQ.syncSend(TOPIC, order, sendTimeoutMillis);
        if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
            throw new IllegalStateException("RocketMQ did not confirm order " + order.getId());
        }
        Long acknowledged = redis.execute(ACK_AND_DELETE, List.of(OrderReservationStore.STREAM_KEY),
                GROUP, record.getId().getValue());
        if (acknowledged == null) {
            throw new IllegalStateException("Missing Stream acknowledgment for order " + order.getId());
        }
    }

    private static Long positiveId(Map<String, String> values, String field) {
        String value = values.get(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing stream field " + field);
        }
        long id = Long.parseLong(value);
        if (id <= 0) {
            throw new IllegalArgumentException("Invalid stream field " + field);
        }
        return id;
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(pollMillis + sendTimeoutMillis + 1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() { return running; }

    @Override
    public boolean isAutoStartup() { return enabled; }

    @Override
    public int getPhase() { return Integer.MAX_VALUE - 1; }
}
