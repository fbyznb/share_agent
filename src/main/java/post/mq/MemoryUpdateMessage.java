package post.mq;

import java.util.List;

import post.model.Message;

/**
 * 一轮完整对话的记忆更新数据。历史快照仅用于长期记忆抽取，不重复写入会话。
 */
public record MemoryUpdateMessage(
        Long conversationId,
        Integer version,
        String question,
        String answer,
        List<Message> conversationMemory
) {
    public MemoryUpdateMessage {
        conversationMemory = conversationMemory == null ? List.of() : List.copyOf(conversationMemory);
    }
}
