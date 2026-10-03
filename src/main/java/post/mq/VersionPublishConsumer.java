package post.mq;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.io.IOException;
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
    private RBloomFilter<Long> postsBloomFilter;
    @Autowired
    private PostMapper postMapper;
    @Autowired
    private ContentMapper contentMapper;
    @Autowired
    private ChatService chatService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ElasticsearchClient elasticsearchClient;
    @Value("${spring.ai.vectorstore.elasticsearch.index-name}")
    private String indexName;

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
            // 已有当前待发布版本的切片时，跳过向量写入并继续发布。
            if (content != null && !hasVersionChunks(pav.postId, pav.version + 1)) {
                chatService.saveVectorStore(content.getContent(),pav.version+1,pav.postId);
            }
            if(post.getStatus() == 1){
                postsBloomFilter.add(pav.postId);
            }
            postMapper.publishVersion(pav.postId, pav.version);
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

    private boolean hasVersionChunks(Long postId, Integer version) {
        try {
            SearchResponse<Void> response = elasticsearchClient.search(
                    search -> search
                            .index(indexName)
                            .size(1)
                            .source(source -> source.fetch(false))
                            .trackTotalHits(total -> total.enabled(false))
                            .allowPartialSearchResults(false)
                            .query(query -> query.bool(bool -> bool
                                    .filter(filter -> filter.term(term -> term
                                            .field("metadata.postId")
                                            .value(String.valueOf(postId))))
                                    .filter(filter -> filter.term(term -> term
                                            .field("metadata.version")
                                            .value(String.valueOf(version)))))),
                    Void.class
            );
            if (response.timedOut()) {
                throw new IllegalStateException("查询文章版本切片超时");
            }
            return !response.hits().hits().isEmpty();
        } catch (IOException exception) {
            throw new IllegalStateException("查询文章版本切片失败", exception);
        }
    }

}
