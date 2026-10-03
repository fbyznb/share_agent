package sku.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import sku.schema.CounterSchema;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@RocketMQMessageListener(
        topic = "canal_outbox",
        consumerGroup = "canal_outbox_consumer_group",
        selectorExpression = "*",
        messageModel = MessageModel.CLUSTERING,
        consumeMode = ConsumeMode.CONCURRENTLY
)
public class CanalOutboxConsumer implements RocketMQListener<Outbox>,
        RocketMQPushConsumerLifecycleListener,
        MessageListenerConcurrently {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanalOutboxConsumer.class);

    private static final String SDS_KEY_PREFIX = "sds:sku:";
    private static final int SDS_FIELD_COUNT = 2;
    private static final long APPLY_MISSING = -1L;
    private static final long APPLY_DUPLICATE = 0L;
    private static final long APPLY_UPDATED = 1L;
    private static final String COUNT_ACTIVE_FAV_SQL =
            "SELECT COUNT(*) FROM fav WHERE sku_id = ? AND status = 1";

    /**
     * 返回值：-1 表示 SDS 缺失，0 表示事件重复，
     * 1 表示增量已应用。无论 SDS 是否存在，幂等键都使用
     * 同一个值 1；已标记但 SDS 仍缺失时继续进入重建。
     */
    static final DefaultRedisScript<Long> APPLY_COUNTER_LUA =
            new DefaultRedisScript<>("""
                    local field = tonumber(ARGV[1])
                    local delta = tonumber(ARGV[2])
                    local dedupeTtl = tonumber(ARGV[3])

                    if not field or field % 1 ~= 0
                            or field < 1 or field > 2 then
                        return redis.error_reply('invalid counter field')
                    end
                    if not delta or delta % 1 ~= 0
                            or delta < -2147483648 or delta > 2147483647 then
                        return redis.error_reply('invalid counter delta')
                    end
                    if not dedupeTtl or dedupeTtl % 1 ~= 0
                            or dedupeTtl <= 0 then
                        return redis.error_reply('invalid dedupe ttl')
                    end

                    local dedupeReply = redis.pcall('GET', KEYS[2])
                    if type(dedupeReply) == 'table'
                            and dedupeReply.err then
                        return dedupeReply
                    end
                    local sdsReply = redis.pcall('GET', KEYS[1])
                    if type(sdsReply) == 'table' and sdsReply.err then
                        return sdsReply
                    end
                    if sdsReply and string.len(sdsReply) ~= 8 then
                        return redis.error_reply(
                            'invalid SDS length for ' .. KEYS[1]
                        )
                    end

                    if dedupeReply then
                        if not sdsReply then
                            return -1
                        end
                        return 0
                    end

                    local claimed = redis.call(
                        'SET', KEYS[2], '1', 'EX', ARGV[3], 'NX'
                    )
                    if not claimed then
                        return 0
                    end

                    if not sdsReply then
                        return -1
                    end

                    local result = redis.pcall(
                        'BITFIELD',
                        KEYS[1],
                        'OVERFLOW',
                        'FAIL',
                        'INCRBY',
                        'i32',
                        '#' .. (field - 1),
                        ARGV[2]
                    )
                    if type(result) == 'table' and result.err then
                        redis.call('DEL', KEYS[2])
                        return result
                    end
                    if result[1] == false then
                        redis.call('DEL', KEYS[2])
                        return redis.error_reply(
                            'counter overflow for ' .. KEYS[1]
                        )
                    end

                    return 1
                    """, Long.class);

    /**
     * 将数据库中的 fav 绝对值发布到 SDS。幂等键已由
     * APPLY_COUNTER_LUA 统一设置为 1；重建成功后只刷新 TTL。
     * SDS 已存在时只覆盖 fav 字段，保留 turn 字段。
     */
    static final DefaultRedisScript<Long> REBUILD_FAV_LUA =
            new DefaultRedisScript<>("""
                    local fav = tonumber(ARGV[1])
                    local dedupeTtl = tonumber(ARGV[2])
                    if not fav or fav % 1 ~= 0
                            or fav < 0 or fav > 2147483647 then
                        return redis.error_reply('invalid fav snapshot')
                    end
                    if not dedupeTtl or dedupeTtl % 1 ~= 0
                            or dedupeTtl <= 0 then
                        return redis.error_reply('invalid dedupe ttl')
                    end

                    local sdsReply = redis.pcall('GET', KEYS[1])
                    if type(sdsReply) == 'table' and sdsReply.err then
                        return sdsReply
                    end
                    if sdsReply and string.len(sdsReply) ~= 8 then
                        return redis.error_reply(
                            'invalid SDS length for ' .. KEYS[1]
                        )
                    end

                    local created = false
                    if not sdsReply then
                        local initialized = redis.pcall(
                            'SET', KEYS[1], string.rep(string.char(0), 8)
                        )
                        if type(initialized) == 'table'
                                and initialized.err then
                            return initialized
                        end
                        created = true
                    end

                    local result = redis.pcall(
                        'BITFIELD',
                        KEYS[1],
                        'OVERFLOW',
                        'FAIL',
                        'SET', 'i32', '#1', ARGV[1]
                    )
                    if type(result) == 'table' and result.err then
                        if created then redis.call('DEL', KEYS[1]) end
                        return result
                    end
                    if result[1] == false then
                        if created then redis.call('DEL', KEYS[1]) end
                        return redis.error_reply('fav snapshot overflow')
                    end

                    redis.call(
                        'SET', KEYS[2], '1', 'EX', ARGV[2]
                    )
                    return 1
                    """, Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final RedissonClient redisson;
    private final JdbcTemplate jdbcTemplate;
    private final long deduplicationTtlSeconds;

    public CanalOutboxConsumer(
            StringRedisTemplate redis,
            ObjectMapper objectMapper,
            RedissonClient redisson,
            JdbcTemplate jdbcTemplate,
            @Value("${sku.counter.dedupe-ttl-seconds:604800}")
            long deduplicationTtlSeconds
    ) {
        if (deduplicationTtlSeconds <= 0) {
            throw new IllegalArgumentException(
                    "deduplication TTL must be greater than zero"
            );
        }
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.redisson = redisson;
        this.jdbcTemplate = jdbcTemplate;
        this.deduplicationTtlSeconds = deduplicationTtlSeconds;
    }

    /**
     * Spring 注解监听器的兼容入口。正常返回仍会被 Spring 视为成功；真正的
     * 显式消费结果由 {@link #consumeMessage(List, ConsumeConcurrentlyContext)}
     * 返回。
     */
    @Override
    public void onMessage(Outbox outbox) {
        process(outbox);
    }

    /**
     * Spring 容器启动前，用原生并发监听器替换其自动确认适配器。PushConsumer
     * 的“手动 ACK”是同步返回 CONSUME_SUCCESS；失败则返回 RECONSUME_LATER。
     */
    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.registerMessageListener(this);
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(
            List<MessageExt> messages,
            ConsumeConcurrentlyContext context
    ) {
        if (messages == null || messages.isEmpty()) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        try {
            for (MessageExt message : messages) {
                Outbox outbox = objectMapper.readValue(
                        message.getBody(),
                        Outbox.class
                );
                process(outbox);
            }
            // 仅在全部 Redis 增量或 SDS 重建完成后显式确认。
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception ex) {
            MessageExt first = messages.get(0);
            LOGGER.warn(
                    "Counter event consume failed; retry later: msgId={}, "
                            + "reconsumeTimes={}",
                    first.getMsgId(),
                    first.getReconsumeTimes(),
                    ex
            );
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    private void process(Outbox outbox) {
        if (outbox == null
                || !CounterEvent.class.getSimpleName().equals(
                outbox.getEventType()
        )) {
            return;
        }

        CounterEvent event;
        try {
            event = objectMapper.readValue(
                    outbox.getPayload(),
                    CounterEvent.class
            );
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(
                    "Invalid CounterEvent payload",
                    ex
            );
        }
        validate(outbox, event);

        String sdsKey = SDS_KEY_PREFIX + event.sid;
        // The hash tag is the complete legacy SDS key, so both keys share a
        // Redis Cluster slot without changing the existing SDS key schema.
        String deduplicationKey =
                "dedupe:{" + sdsKey + "}:event:" + event.eventId;
        long result = requireScriptResult(redis.execute(
                APPLY_COUNTER_LUA,
                List.of(sdsKey, deduplicationKey),
                String.valueOf(event.idx),
                String.valueOf(event.delta),
                String.valueOf(deduplicationTtlSeconds)
        ));

        if (result == APPLY_UPDATED || result == APPLY_DUPLICATE) {
            return;
        }
        if (result != APPLY_MISSING) {
            throw new IllegalStateException(
                    "Unexpected counter script result: " + result
            );
        }

        // SDS 缺失：在与定时校准共用的修复锁内按数据库绝对值重建。
        rebuildMissingSds(
                event,
                sdsKey,
                deduplicationKey
        );
    }

    private void rebuildMissingSds(
            CounterEvent event,
            String sdsKey,
            String deduplicationKey
    ) {
        String lockKey = "repair:{" + sdsKey + "}";
        RLock lock = redisson.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0L, TimeUnit.MILLISECONDS);
            if (!locked) {
                throw new IllegalStateException(
                        "SDS rebuild is already in progress for " + sdsKey
                );
            }
            long favCount = loadFavCount(event.sid);
            validateFavCount(favCount, event.sid);
            long rebuilt = requireScriptResult(redis.execute(
                    REBUILD_FAV_LUA,
                    List.of(sdsKey, deduplicationKey),
                    String.valueOf(favCount),
                    String.valueOf(deduplicationTtlSeconds)
            ));
            if (rebuilt != 0L && rebuilt != 1L) {
                throw new IllegalStateException(
                        "Unexpected SDS rebuild result: " + rebuilt
                );
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for SDS rebuild lock",
                    ex
            );
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private long loadFavCount(long skuId) {
        Long count = jdbcTemplate.queryForObject(
                COUNT_ACTIVE_FAV_SQL,
                Long.class,
                skuId
        );
        if (count == null) {
            throw new IllegalStateException(
                    "Database returned no favorite count for SKU " + skuId
            );
        }
        return count;
    }

    private long requireScriptResult(Long result) {
        if (result == null) {
            throw new IllegalStateException("Redis script returned no result");
        }
        return result;
    }

    private void validateFavCount(long favCount, long skuId) {
        if (favCount < 0 || favCount > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Favorite count is outside the SDS i32 range for SKU "
                            + skuId
            );
        }
    }

    private void validate(Outbox outbox, CounterEvent event) {
        if (event == null) {
            throw new IllegalArgumentException(
                    "CounterEvent payload must not be null"
            );
        }
        if (event.sid <= 0) {
            throw new IllegalArgumentException(
                    "counter subject id must be greater than zero"
            );
        }
        if (event.idx < 1 || event.idx > SDS_FIELD_COUNT) {
            throw new IllegalArgumentException(
                    "counter field index must be between 1 and "
                            + SDS_FIELD_COUNT
            );
        }

        Integer expectedIndex = CounterSchema.NAME_TO_IDX.get(event.metric);
        if (expectedIndex == null || expectedIndex != event.idx) {
            throw new IllegalArgumentException(
                    "counter metric does not match field index"
            );
        }
        if (event.eventId <= 0) {
            throw new IllegalArgumentException(
                    "counter event id must be greater than zero"
            );
        }
        if (outbox.getEventId() == null
                || outbox.getEventId().longValue() != event.eventId) {
            throw new IllegalArgumentException(
                    "outbox event id does not match payload event id"
            );
        }
    }
}
