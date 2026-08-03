package post.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;

import post.memory.DatabaseChatMemoryRepository;
import post.memory.SummarizingWindowChatMemory;

@Configuration
public class SpringAIConfig {

    @Bean
    public ChatClient chatClient(ChatClient.Builder chatClientBuilder) {
        return chatClientBuilder
                .defaultAdvisors(new SimpleLoggerAdvisor())
                .build();
    }

    @Bean
    public SummarizingWindowChatMemory summarizingWindowChatMemory(
            DatabaseChatMemoryRepository chatMemoryRepository,
            ChatClient chatClient
    ) {
        return SummarizingWindowChatMemory.builder()
                .chatMemoryRepository(chatMemoryRepository)
                .chatClient(chatClient)
                .build();
    }
}