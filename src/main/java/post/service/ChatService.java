package post.service;


import reactor.core.publisher.Flux;

public interface ChatService {
    Long createConversation(Long userId, Long postId);
    Flux<String> chat(Long userId, Long conversationId, String question, Long postId, Integer version);
    void saveVectorStore(String content,Integer version,long postId);
}
