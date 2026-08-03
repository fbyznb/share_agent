package post.service;


import reactor.core.publisher.Flux;

public interface ChatService {
    Flux<String> chat(Long userId,Long sessionId,Long taskId,String question,Long postId,Integer version,Integer topK,Integer maxTokens);
    void saveVectorStore(String content,Integer version,long postId);
}
