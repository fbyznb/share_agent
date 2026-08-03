package post.service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.messaging.Message;
import post.controller.dto.InsertResponse;
import post.controller.dto.SelectResponse;
import post.controller.dto.UpdateResponse;
import post.mapper.ContentMapper;
import post.mapper.PostMapper;
import post.model.Content;
import post.model.Post;
import post.mq.Pav;

import java.util.List;


@Service
public class PostServiceImpl implements PostService {

    private static final String CACHE_REFILL_LUA = """
            -- KEYS[1]：数据 Key，类型为 String
            -- KEYS[2]：版本屏障 Key，类型为 String
            -- ARGV[1]：待回填数据的版本号
            -- ARGV[2]：待回填的完整 JSON 数据

            local inputVersion = ARGV[1]
            local payload = ARGV[2]
            local barrierVersion = redis.call('GET', KEYS[2])

            -- 屏障存在时，旧版本查询结果不能覆盖新版本缓存
            if barrierVersion
                and tonumber(inputVersion) < tonumber(barrierVersion) then
                return 0
            end

            -- 屏障不存在或输入版本不小于屏障版本时更新屏障
            redis.call('SET', KEYS[2], inputVersion, 'EX', 1800)

            -- 查询结果非空时才回填数据，数据 TTL 为 15 分钟
            if payload and payload ~= '' and payload ~= 'null' then
                redis.call('SET', KEYS[1], payload, 'EX', 900)
            end

            return 1
            """;
    
    @Autowired
    private PostMapper postMapper;
    @Autowired
    private ContentMapper contentMapper;
    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    
    @Override
    @Transactional(rollbackFor = Exception.class)
    public InsertResponse insertPost(Long userId, String title) {
        Post post = new Post();
        post.setUserId(userId);
        post.setTitle(title);
        post.setStatus(0);
        post.setVersion(0);
        postMapper.insert(post);
        contentMapper.insert(new Content(post.getPostId(),post.getVersion()+1,""));
        InsertResponse insertResponse = new InsertResponse();
        insertResponse.postId = post.getPostId();
        insertResponse.version = post.getVersion();
        insertResponse.status = post.getStatus();
        insertResponse.success = true;
        return insertResponse;
    }



    @Override
    @Transactional(rollbackFor = Exception.class)
    public UpdateResponse selectContentByPost(Long postId) {
        Post post = postMapper.select(postId);
        Content content = contentMapper.selectContentByPost(postId,post.getVersion()+1);
        if(content == null){
            content = new Content(postId, post.getVersion()+1, "");
            contentMapper.insert(content);
        }
        UpdateResponse updateResponse = new UpdateResponse();
        updateResponse.version = post.getVersion();
        updateResponse.status = post.getStatus();
        updateResponse.content = content.getContent();
        return updateResponse;     
    }


    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean publishContent(String content, Long postId, Integer version, Integer status) {
        int t = postMapper.update(postId, version, status);
        if(t == 0){
            return false;
        }
        int contentUpdate = contentMapper.update(postId, version + 1, content);
        if(contentUpdate == 0){
            throw new RuntimeException("Content update failed: no rows affected");
        }
        Pav pv = new Pav(postId,version);
        Message<Pav> message = MessageBuilder.withPayload(pv).build();
        rocketMQTemplate.syncSend("version_publish", message, 10000L, 3);
        return true;
    }
    
    @Override
    public SelectResponse selectPost(Long postId) {
        if (postId == null) {
            return null;
        }
        RBloomFilter<Long> postsBloomFilter =
                redissonClient.getBloomFilter("bloom:posts");
        if (!postsBloomFilter.contains(postId)) {
            return null;
        }
        String barrierKey = "post:barrier:" + postId;
        String dataKey = "post:data:" + postId;
        List<String> cachedValues = stringRedisTemplate.opsForValue()
                .multiGet(List.of(barrierKey, dataKey));
        CachedResponse cachedResponse = getCachedResponse(cachedValues);
        if (cachedResponse.hit()) {
            return cachedResponse.value();
        }
        RLock lock = redissonClient.getLock("post:" + postId);
        lock.lock();
        try {
            cachedValues = stringRedisTemplate.opsForValue()
                    .multiGet(List.of(barrierKey, dataKey));
            cachedResponse = getCachedResponse(cachedValues);
            if (cachedResponse.hit()) {
                return cachedResponse.value();
            }
            Post post = postMapper.select(postId);
            Content content = null;
            if(post != null){
                content = contentMapper.selectContentByPost(postId,post.getVersion());
            }
            SelectResponse selectResponse = new SelectResponse();
            selectResponse.content = content;
            selectResponse.post = post;
            DefaultRedisScript<Long> redisScript =
                    new DefaultRedisScript<>(CACHE_REFILL_LUA, Long.class);
            stringRedisTemplate.execute(
                    redisScript,
                    List.of(dataKey, barrierKey),
                    String.valueOf(post != null ? post.getVersion() : 0),
                    writeJson(selectResponse)
            );
            return selectResponse;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private CachedResponse getCachedResponse(List<String> cachedValues) {
        if (cachedValues != null
                && !cachedValues.isEmpty()
                && "0".equals(cachedValues.get(0))) {
            return new CachedResponse(true, null);
        }
        if (cachedValues == null
                || cachedValues.size() < 2
                || cachedValues.get(0) == null
                || cachedValues.get(1) == null) {
            return new CachedResponse(false, null);
        }
        try {
            SelectResponse response =
                    objectMapper.readValue(cachedValues.get(1), SelectResponse.class);
            return new CachedResponse(true, response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize cached post", e);
        }
    }

    private record CachedResponse(boolean hit, SelectResponse value) {
    }

    private String writeJson(SelectResponse selectResponse) {
        try {
            return objectMapper.writeValueAsString(selectResponse);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize post for cache", e);
        }
    }
}
