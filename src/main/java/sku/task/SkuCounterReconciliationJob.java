package sku.task;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 每日以数据库中的收藏事实为准，校准 Redis 中的 SKU 计数 SDS。
 *
 * <p>当前数据库没有 turn 计数的事实来源，因此任务只校准 fav 字段，
 * 并保留 SDS 中已有的 turn 字段。</p>
 */
@Component
@ConditionalOnProperty(
        prefix = "sku.counter.reconciliation",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class SkuCounterReconciliationJob {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SkuCounterReconciliationJob.class);

    private static final String SDS_KEY_PREFIX = "sds:sku:";
    private static final String REPAIR_LOCK_PREFIX = "repair:{";
    private static final String REPAIR_LOCK_SUFFIX = "}";
    private static final String JOB_LOCK_KEY =
            "job:sku-counter:daily-reconciliation";
    private static final int SDS_BYTE_LENGTH = 8;
    private static final int FAV_BYTE_OFFSET = Integer.BYTES;
    private static final String LOAD_SKU_FAV_COUNTS_SQL = """
            SELECT b.id AS sku_id, COUNT(f.id) AS fav_count
            FROM (
                SELECT id
                FROM sku
                WHERE id > ?
                ORDER BY id
                LIMIT ?
            ) b
            LEFT JOIN fav f
                ON f.sku_id = b.id
               AND f.status = 1
            GROUP BY b.id
            ORDER BY b.id
            """;
    private static final String COUNT_ACTIVE_FAV_SQL =
            "SELECT COUNT(*) FROM fav WHERE sku_id = ? AND status = 1";

    /**
     * 返回 1 表示 SDS 被创建或 fav 值被修正，0 表示原值已一致。
     * SDS 已存在时只写第二个 i32，避免覆盖没有数据库事实来源的 turn。
     */
    static final DefaultRedisScript<Long> RECONCILE_FAV_LUA =
            new DefaultRedisScript<>("""
                    local fav = tonumber(ARGV[1])
                    if not fav or fav % 1 ~= 0
                            or fav < 0 or fav > 2147483647 then
                        return redis.error_reply('invalid fav snapshot')
                    end

                    local sdsReply = redis.pcall('GET', KEYS[1])
                    if type(sdsReply) == 'table' and sdsReply.err then
                        return sdsReply
                    end
                    local created = false
                    if not sdsReply then
                        redis.call(
                            'SET', KEYS[1], string.rep(string.char(0), 8)
                        )
                        created = true
                    elseif string.len(sdsReply) ~= 8 then
                        return redis.error_reply(
                            'invalid SDS length for ' .. KEYS[1]
                        )
                    end

                    local current = redis.call(
                        'BITFIELD', KEYS[1], 'GET', 'i32', '#1'
                    )[1]
                    if current == fav then
                        return created and 1 or 0
                    end

                    local result = redis.call(
                        'BITFIELD', KEYS[1],
                        'OVERFLOW', 'FAIL',
                        'SET', 'i32', '#1', ARGV[1]
                    )
                    if result[1] == false then
                        return redis.error_reply('fav snapshot overflow')
                    end
                    return 1
                    """, Long.class);

    private final JdbcTemplate jdbcTemplate;
    private final StringRedisTemplate redis;
    private final RedissonClient redisson;
    private final int batchSize;

    public SkuCounterReconciliationJob(
            JdbcTemplate jdbcTemplate,
            StringRedisTemplate redis,
            RedissonClient redisson,
            @Value("${sku.counter.reconciliation.batch-size:500}")
            int batchSize
    ) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException(
                    "reconciliation batch size must be positive"
            );
        }
        this.jdbcTemplate = jdbcTemplate;
        this.redis = redis;
        this.redisson = redisson;
        this.batchSize = batchSize;
    }

    @Scheduled(
            cron = "${sku.counter.reconciliation.cron:0 0 3 * * *}",
            zone = "${sku.counter.reconciliation.zone:Asia/Shanghai}"
    )
    public void reconcileDaily() {
        RLock jobLock = redisson.getLock(JOB_LOCK_KEY);
        boolean locked = false;
        try {
            locked = jobLock.tryLock(0L, TimeUnit.MILLISECONDS);
            if (!locked) {
                LOGGER.info(
                        "Skip daily SKU counter reconciliation: another "
                                + "instance is running"
                );
                return;
            }
            reconcileAllSkus();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn(
                    "Daily SKU counter reconciliation was interrupted",
                    ex
            );
        } catch (RuntimeException ex) {
            LOGGER.error("Daily SKU counter reconciliation failed", ex);
        } finally {
            if (locked && jobLock.isHeldByCurrentThread()) {
                jobLock.unlock();
            }
        }
    }

    private void reconcileAllSkus() {
        long lastSkuId = 0L;
        long scanned = 0L;
        long corrected = 0L;
        long skipped = 0L;
        long failed = 0L;

        while (true) {
            List<SkuFavSnapshot> snapshots = jdbcTemplate.query(
                    LOAD_SKU_FAV_COUNTS_SQL,
                    (resultSet, rowNum) -> new SkuFavSnapshot(
                            resultSet.getLong("sku_id"),
                            resultSet.getLong("fav_count")
                    ),
                    lastSkuId,
                    batchSize
            );
            if (snapshots.isEmpty()) {
                break;
            }

            List<Object> redisSnapshots = loadRedisSnapshots(snapshots);
            if (redisSnapshots.size() != snapshots.size()) {
                throw new IllegalStateException(
                        "Redis pipeline result count does not match SKU batch"
                );
            }

            for (int index = 0; index < snapshots.size(); index++) {
                SkuFavSnapshot snapshot = snapshots.get(index);
                long skuId = snapshot.skuId();
                if (skuId <= 0) {
                    failed++;
                    LOGGER.warn("Skip invalid SKU id during reconciliation: {}", skuId);
                    continue;
                }
                scanned++;
                try {
                    validateFavCount(snapshot.favCount(), skuId);
                    if (matchesRedisFav(
                            redisSnapshots.get(index),
                            snapshot.favCount()
                    )) {
                        continue;
                    }

                    ReconciliationResult result = reconcileSku(skuId);
                    if (result == ReconciliationResult.CORRECTED) {
                        corrected++;
                    } else if (result == ReconciliationResult.SKIPPED) {
                        skipped++;
                    }
                } catch (RuntimeException ex) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw ex;
                    }
                    failed++;
                    LOGGER.error(
                            "Failed to reconcile SKU counter: skuId={}",
                            skuId,
                            ex
                    );
                }
            }

            lastSkuId = snapshots.get(snapshots.size() - 1).skuId();
            if (snapshots.size() < batchSize) {
                break;
            }
        }

        LOGGER.info(
                "Daily SKU counter reconciliation finished: scanned={}, "
                        + "corrected={}, skipped={}, failed={}",
                scanned,
                corrected,
                skipped,
                failed
        );
    }

    private List<Object> loadRedisSnapshots(
            List<SkuFavSnapshot> snapshots
    ) {
        RedisSerializer<String> keySerializer = redis.getStringSerializer();
        return redis.executePipelined(
                (RedisCallback<Object>) connection -> {
                    for (SkuFavSnapshot snapshot : snapshots) {
                        byte[] key = keySerializer.serialize(
                                SDS_KEY_PREFIX + snapshot.skuId()
                        );
                        if (key == null) {
                            throw new IllegalStateException(
                                    "Redis key serializer returned no data"
                            );
                        }
                        connection.stringCommands().get(key);
                    }
                    return null;
                },
                RedisSerializer.byteArray()
        );
    }

    private boolean matchesRedisFav(Object rawValue, long expectedFav) {
        if (!(rawValue instanceof byte[] sds)
                || sds.length != SDS_BYTE_LENGTH) {
            return false;
        }
        int redisFav = ByteBuffer.wrap(sds)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt(FAV_BYTE_OFFSET);
        return redisFav == expectedFav;
    }

    private ReconciliationResult reconcileSku(long skuId) {
        String sdsKey = SDS_KEY_PREFIX + skuId;
        // 与 CanalOutboxConsumer#rebuildMissingSds 使用同一把锁。
        RLock repairLock = redisson.getLock(
                REPAIR_LOCK_PREFIX + sdsKey + REPAIR_LOCK_SUFFIX
        );
        boolean locked = false;
        try {
            locked = repairLock.tryLock(0L, TimeUnit.MILLISECONDS);
            if (!locked) {
                LOGGER.debug(
                        "Skip busy SKU counter during reconciliation: skuId={}",
                        skuId
                );
                return ReconciliationResult.SKIPPED;
            }

            Long favCount = jdbcTemplate.queryForObject(
                    COUNT_ACTIVE_FAV_SQL,
                    Long.class,
                    skuId
            );
            validateFavCount(favCount, skuId);

            Long result = redis.execute(
                    RECONCILE_FAV_LUA,
                    List.of(sdsKey),
                    String.valueOf(favCount)
            );
            if (result == null) {
                throw new IllegalStateException(
                        "Redis reconciliation script returned no result"
                );
            }
            if (result == 1L) {
                return ReconciliationResult.CORRECTED;
            }
            if (result == 0L) {
                return ReconciliationResult.UNCHANGED;
            }
            throw new IllegalStateException(
                    "Unexpected Redis reconciliation result: " + result
            );
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for SKU repair lock",
                    ex
            );
        } finally {
            if (locked && repairLock.isHeldByCurrentThread()) {
                repairLock.unlock();
            }
        }
    }

    private void validateFavCount(Long favCount, long skuId) {
        if (favCount == null
                || favCount < 0
                || favCount > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Favorite count is outside the SDS i32 range for SKU "
                            + skuId
            );
        }
    }

    private enum ReconciliationResult {
        CORRECTED,
        UNCHANGED,
        SKIPPED
    }

    record SkuFavSnapshot(long skuId, long favCount) {
    }
}
