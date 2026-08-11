package sku.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@RocketMQMessageListener(
        topic = "canal_outbox",
        consumerGroup = "canal_outbox_consumer_group",
        selectorExpression = "*",
        messageModel = MessageModel.CLUSTERING,
        consumeMode = ConsumeMode.CONCURRENTLY
)
public class CanalOutboxConsumer
        implements RocketMQListener<sku.mq.Outbox> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanalOutboxConsumer.class);

    private static final String AGGREGATION_INDEX_KEY = "aggs:sku";
    private static final String AGGREGATION_KEY_PREFIX = "agg:sku:";
    private static final String SDS_KEY_PREFIX = "sds:sku:";
    private static final int SDS_FIELD_COUNT = 2;

    private static final DefaultRedisScript<Long> AGGREGATE_LUA =
            new DefaultRedisScript<>("""
                    redis.call('SADD', KEYS[2], KEYS[1])
                    redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
                    return 1
                    """, Long.class);

    private static final DefaultRedisScript<Long> FLUSH_LUA =
            new DefaultRedisScript<>("""
                    local sds = redis.call('GET', KEYS[3])
                    local bucket = redis.call('HGETALL', KEYS[2])

                    if #bucket == 0 then
                        redis.call('SREM', KEYS[1], KEYS[2])
                        return 0
                    end

                    if sds and string.len(sds) ~= 8 then
                        return redis.error_reply('invalid SDS length for ' .. KEYS[3])
                    end

                    local bitfieldArgs = {
                        'BITFIELD', KEYS[3], 'OVERFLOW', 'WRAP'
                    }

                    for i = 1, #bucket, 2 do
                        local field = tonumber(bucket[i])
                        if not field or field % 1 ~= 0 or field < 1 or field > 2 then
                            return redis.error_reply(
                                'invalid counter field for ' .. KEYS[2]
                            )
                        end

                        table.insert(bitfieldArgs, 'INCRBY')
                        table.insert(bitfieldArgs, 'i32')
                        table.insert(bitfieldArgs, '#' .. (field - 1))
                        table.insert(bitfieldArgs, bucket[i + 1])
                    end

                    if not sds then
                        redis.call('SET', KEYS[3], string.rep(string.char(0), 8))
                    end

                    redis.call(unpack(bitfieldArgs))
                    redis.call('DEL', KEYS[2])
                    redis.call('SREM', KEYS[1], KEYS[2])
                    return #bucket / 2
                    """, Long.class);

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void onMessage(sku.mq.Outbox outbox) {
        //判断outbox中的eventType是CountEvent则将利用outbox对象的payload转换为CounterEvent对象继续执行对应逻辑
        if (outbox == null
                || !CounterEvent.class.getSimpleName().equals(outbox.getEventType())) {
            return;
        }

        CounterEvent evt;
        try {
            evt = objectMapper.readValue(outbox.getPayload(), CounterEvent.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid CounterEvent payload", ex);
        }

        if (evt.idx < 1 || evt.idx > SDS_FIELD_COUNT) {
            throw new IllegalArgumentException(
                    "counter field index must be between 1 and " + SDS_FIELD_COUNT
            );
        }

        String aggKey = AGGREGATION_KEY_PREFIX + evt.sid;
        String aggsKey = AGGREGATION_INDEX_KEY;
        //Redis中aggKey是Hash结构，aggsKey是Set结构；执行Lua脚本：redis.opsForHash().increment(aggKey, evt.idx, evt.delta); 将aggKey加入到aggsKey中
        redis.execute(
                AGGREGATE_LUA,
                List.of(aggKey, aggsKey),
                String.valueOf(evt.idx),
                String.valueOf(evt.delta)
        );
    }

    @Scheduled(fixedDelay = 10000L)
    public void flush() {
        //0、索引键：aggs:sku   聚合桶键：agg:sku{sid}  SDS键：sds:sku:{sid}（SDS键是String类型，以二进制形式存储两个字段，共8个字节，每4个字节存储一个字段的值，字段标识对应聚合桶键的值中的field（代表是第几个字段））
        //1、得到aggsKey中所有的键
        Set<String> aggKeys = redis.opsForSet().members(AGGREGATION_INDEX_KEY);
        if (aggKeys == null || aggKeys.isEmpty()) {
            return;
        }

        //2、循环遍历1中所有键
        for (String aggKey : aggKeys) {
            if (aggKey == null
                    || !aggKey.startsWith(AGGREGATION_KEY_PREFIX)
                    || aggKey.length() == AGGREGATION_KEY_PREFIX.length()) {
                LOGGER.warn("Ignore invalid aggregation key: {}", aggKey);
                continue;
            }

            String sid = aggKey.substring(AGGREGATION_KEY_PREFIX.length());
            String sdsKey = SDS_KEY_PREFIX + sid;

            //3、循环中执行Lua脚本：读SDS；读聚合桶；循环遍历聚合桶中各字段将各字段值累加到SDS；删除聚合桶键；将聚合桶键从索引值中移除
            try {
                redis.execute(
                        FLUSH_LUA,
                        List.of(AGGREGATION_INDEX_KEY, aggKey, sdsKey)
                );
            } catch (RuntimeException ex) {
                // 脚本只在累加完成后删除桶，失败时保留数据供下次调度重试。
                LOGGER.error("Failed to flush aggregation bucket {}", aggKey, ex);
            }
        }
    }
}
