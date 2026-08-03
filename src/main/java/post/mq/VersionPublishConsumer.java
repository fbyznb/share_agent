package post.mq;

import post.mapper.PostMapper;
import post.model.Content;
import post.model.Post;
import post.mapper.ContentMapper;
import post.service.ChatService;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


@Component
@RocketMQMessageListener(
        topic = "version_publish",
        consumerGroup = "publish_content_consumer_group",
        selectorExpression = "*",
        messageModel = org.apache.rocketmq.spring.annotation.MessageModel.CLUSTERING,
        consumeMode = org.apache.rocketmq.spring.annotation.ConsumeMode.CONCURRENTLY,
        consumeThreadNumber = 20,
        consumeThreadMax = 64,
        maxReconsumeTimes = 3
)
public class VersionPublishConsumer implements RocketMQListener<Pav> {

    private static final Logger logger = LoggerFactory.getLogger(VersionPublishConsumer.class);
    private static final DefaultRedisScript<Long> PUBLISH_CACHE_LUA =
            new DefaultRedisScript<>("""
                    local publishVersion = tonumber(ARGV[1])
                    local barrierVersion = redis.call('GET', KEYS[1])

                    if not barrierVersion
                        or publishVersion > tonumber(barrierVersion) then
                        redis.call('SET', KEYS[1], ARGV[1], 'EX', 1800)
                        redis.call('DEL', KEYS[2])
                        return 1
                    end

                    return 0
                    """, Long.class);

    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private PostMapper postMapper;
    @Autowired
    private ContentMapper contentMapper;
    @Autowired
    private ChatService chatService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(Pav pav) {
        RLock lock = redissonClient.getLock(pav.postId + ":" + pav.version);
        boolean isLock = false;
        try {
            isLock = lock.tryLock();
            if(!isLock){
                throw new RuntimeException("lock failed");
            }
            Post post = postMapper.selectVersionAndStatus(pav.postId);
            if (post == null || !post.getVersion().equals(pav.version)) {
                return;
            }
            Content content = contentMapper.selectContentByPost(pav.postId, pav.version + 1);
            if (content != null) {
                chatService.saveVectorStore(content.getContent(),pav.version+1,pav.postId);
            }
            postMapper.publishVersion(pav.postId, pav.version);
            if(post.getStatus() == 1){
                RBloomFilter<Long> postsBloomFilter =
                    redissonClient.getBloomFilter("bloom:posts");
                postsBloomFilter.add(pav.postId);
            }
            int publishedVersion = pav.version + 1;
            stringRedisTemplate.execute(
                    PUBLISH_CACHE_LUA,
                    List.of(
                            "post:barrier:" + pav.postId,
                            "post:data:" + pav.postId
                    ),
                    String.valueOf(publishedVersion)
            );
        } finally {
            if (isLock && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

}