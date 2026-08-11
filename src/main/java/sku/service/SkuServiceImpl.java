package sku.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sku.mq.CounterEvent;
import sku.schema.CounterSchema;

@Service
public class SkuServiceImpl implements SkuService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SkuServiceImpl.class);
    private static final String COUNTER_EVENT_TYPE =
            CounterEvent.class.getSimpleName();
    private static final long SNOWFLAKE_EPOCH = 1735689600000L;
    private static final long SEQUENCE_MASK = 0xFFFL;
    private static final int WORKER_ID_SHIFT = 12;
    private static final int TIMESTAMP_SHIFT = 22;
    private static final long WORKER_ID = 0L;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private long lastTimestamp = -1L;
    private long sequence;

    public SkuServiceImpl(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public boolean fav(Long skuId, Long userId, Boolean isFav) {
        if (skuId == null || userId == null || isFav == null) {
            return false;
        }

        try {
            int affectedRows;
            if (isFav) {
                affectedRows = jdbcTemplate.update(
                        """
                        INSERT INTO fav (sku_id, user_id, status)
                        VALUES (?, ?, 1)
                        ON DUPLICATE KEY UPDATE status = 1
                        """,
                        skuId,
                        userId
                );
            } else {
                affectedRows = jdbcTemplate.update(
                        """
                        UPDATE fav
                        SET status = 0
                        WHERE sku_id = ? AND user_id = ? AND status = 1
                        """,
                        skuId,
                        userId
                );
            }

            if (affectedRows == 0) {
                // 目标状态已经存在，按幂等请求处理，无需重复发送计数事件。
                return true;
            }
            if ((isFav && affectedRows != 1 && affectedRows != 2)
                    || (!isFav && affectedRows != 1)) {
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                return false;
            }

            long eventId = nextEventId();
            CounterEvent event = new CounterEvent();
            event.sid = skuId;
            event.metric = "fav";
            event.idx = CounterSchema.IDX_FAV;
            event.delta = isFav ? 1 : -1;
            event.eventId = eventId;

            int outboxRows = jdbcTemplate.update(
                    """
                    INSERT INTO outbox
                        (event_id, payload, status, retry_count, event_type)
                    VALUES (?, ?, 0, 0, ?)
                    """,
                    eventId,
                    objectMapper.writeValueAsString(event),
                    COUNTER_EVENT_TYPE
            );
            if (outboxRows == 1) {
                return true;
            }
        } catch (DataAccessException | JsonProcessingException ex) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            LOGGER.error(
                    "Failed to change favorite state: skuId={}, userId={}, isFav={}",
                    skuId,
                    userId,
                    isFav,
                    ex
            );
            return false;
        }

        TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        return false;
    }

    private synchronized long nextEventId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            throw new IllegalStateException("System clock moved backwards");
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                timestamp = waitUntilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }

        lastTimestamp = timestamp;
        return ((timestamp - SNOWFLAKE_EPOCH) << TIMESTAMP_SHIFT)
                | (WORKER_ID << WORKER_ID_SHIFT)
                | sequence;
    }

    private long waitUntilNextMillis(long timestamp) {
        long current;
        do {
            current = System.currentTimeMillis();
        } while (current <= timestamp);
        return current;
    }
}
