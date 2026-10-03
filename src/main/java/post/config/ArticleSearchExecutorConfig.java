package post.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.RejectedExecutionException;

@Configuration(proxyBeanMethods = false)
public class ArticleSearchExecutorConfig {

    @Bean("articleSearchExecutor")
    public ThreadPoolTaskExecutor articleSearchExecutor(
            @Value("${article.search.executor.core-size:8}") int coreSize,
            @Value("${article.search.executor.max-size:16}") int maxSize,
            @Value("${article.search.executor.queue-capacity:32}") int queueCapacity
    ) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("article-recall-");
        executor.setRejectedExecutionHandler((task, threadPool) -> {
            if (threadPool.isShutdown()) {
                throw new RejectedExecutionException("articleSearchExecutor 已关闭");
            }
            task.run();
        });
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(10);
        return executor;
    }
}
