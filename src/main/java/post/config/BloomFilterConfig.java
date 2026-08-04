package post.config;

import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BloomFilterConfig {

    private static final String POSTS_BLOOM_FILTER_NAME = "bloom:posts";
    private static final long EXPECTED_INSERTIONS = 1_000_000L;
    private static final double FALSE_POSITIVE_RATE = 0.01D;

    @Bean
    public RBloomFilter<Long> postsBloomFilter(RedissonClient redissonClient) {
        RBloomFilter<Long> bloomFilter =
                redissonClient.getBloomFilter(POSTS_BLOOM_FILTER_NAME);
        bloomFilter.tryInit(EXPECTED_INSERTIONS, FALSE_POSITIVE_RATE);
        return bloomFilter;
    }
}
