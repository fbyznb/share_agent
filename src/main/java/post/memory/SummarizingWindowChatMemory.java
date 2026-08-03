package post.memory;

import java.util.ArrayList;
import java.util.List;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import post.memory.DatabaseChatMemoryRepository.ConversationState;
import post.memory.DatabaseChatMemoryRepository.StoredMessage;
import post.model.Message;

/**
 * 保留最近消息窗口，并将退出窗口的消息增量合并到历史摘要中。
 *
 * <p>摘要存储在 chat.summary 中，原始消息全部追加到 message 表中。
 * chat.last_message_id 标识已经参与摘要计算的最后一条消息。
 * {@link #get(String)} 返回的第一条消息为历史摘要（如果存在），其后为尚未参与
 * 摘要计算的窗口消息。</p>
 */
public final class SummarizingWindowChatMemory {

    private static final int DEFAULT_MAX_MESSAGES = 20;
    private static final int LOCK_STRIPES = 64;
    private static final String SUMMARY_PREFIX = "以下是较早对话的摘要：\n";
    private static final String DEFAULT_SUMMARY_SYSTEM_PROMPT = """
            你负责维护对话的滚动摘要。请将已有摘要和新退出窗口的消息合并成一份新的摘要。
            保留用户目标、事实、约束、决定、未完成事项和必要上下文；删除重复和无关内容。
            只输出摘要正文，不要添加标题或解释。
            """;

    private final DatabaseChatMemoryRepository chatMemoryRepository;
    private final SummaryGenerator summaryGenerator;
    private final int maxMessages;
    private final Object[] locks = new Object[LOCK_STRIPES];

    public SummarizingWindowChatMemory(
            DatabaseChatMemoryRepository chatMemoryRepository,
            SummaryGenerator summaryGenerator,
            int maxMessages
    ) {
        Assert.notNull(chatMemoryRepository, "chatMemoryRepository cannot be null");
        Assert.notNull(summaryGenerator, "summaryGenerator cannot be null");
        Assert.isTrue(maxMessages > 0, "maxMessages must be greater than 0");

        this.chatMemoryRepository = chatMemoryRepository;
        this.summaryGenerator = summaryGenerator;
        this.maxMessages = maxMessages;

        for (int i = 0; i < this.locks.length; i++) {
            this.locks[i] = new Object();
        }
    }

    public void add(String conversationId, List<Message> messages) {
        Assert.hasText(conversationId, "conversationId cannot be null or empty");
        Assert.notNull(messages, "messages cannot be null");
        Assert.noNullElements(messages, "messages cannot contain null elements");

        synchronized (lockFor(conversationId)) {
            ConversationState state =
                    this.chatMemoryRepository.loadConversation(conversationId);
            List<StoredMessage> window = new ArrayList<>(state.messages());
            window.addAll(this.chatMemoryRepository.append(conversationId, messages));

            if (window.size() <= this.maxMessages) {
                return;
            }

            int overflow = window.size() - this.maxMessages;
            List<Message> evictedMessages = window.subList(0, overflow).stream()
                    .map(StoredMessage::message)
                    .toList();
            String newSummary = this.summaryGenerator.summarize(
                    state.summary(),
                    evictedMessages
            );
            if (!StringUtils.hasText(newSummary)) {
                throw new IllegalStateException("summaryGenerator returned an empty summary");
            }

            this.chatMemoryRepository.updateSummary(
                    conversationId,
                    newSummary.trim(),
                    window.get(overflow - 1).id()
            );
        }
    }

    public List<Message> get(String conversationId) {
        Assert.hasText(conversationId, "conversationId cannot be null or empty");
        ConversationState state = this.chatMemoryRepository.loadConversation(conversationId);
        List<Message> result = new ArrayList<>(state.messages().size() + 1);
        if (StringUtils.hasText(state.summary())) {
            result.add(new Message(
                    Long.valueOf(conversationId),
                    null,
                    "system",
                    SUMMARY_PREFIX + state.summary()
            ));
        }
        state.messages().stream()
                .map(StoredMessage::message)
                .forEach(result::add);
        return List.copyOf(result);
    }

    /**
     * 获取窗口外历史消息的摘要；尚未产生摘要时返回 {@code null}。
     */
    public String getSummary(String conversationId) {
        Assert.hasText(conversationId, "conversationId cannot be null or empty");
        return this.chatMemoryRepository.loadConversation(conversationId).summary();
    }

    public void clear(String conversationId) {
        Assert.hasText(conversationId, "conversationId cannot be null or empty");
        synchronized (lockFor(conversationId)) {
            this.chatMemoryRepository.deleteByConversationId(conversationId);
        }
    }

    private Object lockFor(String conversationId) {
        int index = (conversationId.hashCode() & Integer.MAX_VALUE) % this.locks.length;
        return this.locks[index];
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * 将已有摘要和本次退出窗口的消息合并为新摘要。
     */
    @FunctionalInterface
    public interface SummaryGenerator {

        String summarize(String previousSummary, List<Message> evictedMessages);
    }

    public static final class Builder {

        private DatabaseChatMemoryRepository chatMemoryRepository;
        private SummaryGenerator summaryGenerator;
        private int maxMessages = DEFAULT_MAX_MESSAGES;

        private Builder() {
        }

        public Builder chatMemoryRepository(
                DatabaseChatMemoryRepository chatMemoryRepository
        ) {
            this.chatMemoryRepository = chatMemoryRepository;
            return this;
        }

        public Builder summaryGenerator(SummaryGenerator summaryGenerator) {
            this.summaryGenerator = summaryGenerator;
            return this;
        }

        public Builder chatClient(ChatClient chatClient) {
            Assert.notNull(chatClient, "chatClient cannot be null");
            this.summaryGenerator = chatClientSummaryGenerator(chatClient);
            return this;
        }

        public Builder maxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
            return this;
        }

        public SummarizingWindowChatMemory build() {
            Assert.notNull(
                    this.chatMemoryRepository,
                    "chatMemoryRepository must be configured"
            );
            Assert.notNull(
                    this.summaryGenerator,
                    "summaryGenerator or chatClient must be configured"
            );
            return new SummarizingWindowChatMemory(
                    this.chatMemoryRepository,
                    this.summaryGenerator,
                    this.maxMessages
            );
        }
    }

    private static SummaryGenerator chatClientSummaryGenerator(ChatClient chatClient) {
        return (previousSummary, evictedMessages) -> {
            StringBuilder input = new StringBuilder();
            if (StringUtils.hasText(previousSummary)) {
                input.append("已有摘要：\n")
                        .append(previousSummary)
                        .append("\n\n");
            }

            input.append("本次退出窗口的消息：\n");
            for (Message message : evictedMessages) {
                input.append('[')
                        .append(message.getRole())
                        .append("] ")
                        .append(message.getContent() == null ? "" : message.getContent())
                        .append('\n');
            }

            return chatClient.prompt()
                    .system(DEFAULT_SUMMARY_SYSTEM_PROMPT)
                    .user(input.toString())
                    .call()
                    .content();
        };
    }
}
