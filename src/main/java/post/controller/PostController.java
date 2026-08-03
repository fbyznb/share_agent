package post.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;

import post.controller.dto.InsertResponse;
import post.controller.dto.SelectResponse;
import post.controller.dto.UpdateResponse;
import post.service.ChatService;
import post.service.PostService;

@RestController
@RequestMapping("/post")
public class PostController {

    @Autowired
    private PostService postService;

    @Autowired
    private ChatService chatService;

    @RequestMapping(path = "/insertPost")
    public InsertResponse insertPost(
            @RequestParam(value = "userId", required = false) Long userId,
            @RequestParam(value = "title", required = false) String title) {
        return postService.insertPost(userId, title);
    }

    @RequestMapping(path = "/selectContentByPost")
    public UpdateResponse selectContentByPost(
            @RequestParam(value = "postId", required = false) Long postId) {
        return postService.selectContentByPost(postId);
    }

    @RequestMapping(path = "/publishContent")
    public boolean publishContent(
            @RequestParam(value = "content", required = false) String content,
            @RequestParam(value = "postId", required = false) Long postId,
            @RequestParam(value = "version", required = false) Integer version,
            @RequestParam(value = "status", required = false) Integer status) {
        return postService.publishContent(content, postId, version, status);
    }

    @GetMapping(path = "/selectPost")
    public SelectResponse selectPost(@RequestParam(value = "postId", required = false) Long postId) {
        if (postId == null) {
            return null;
        }
        return postService.selectPost(postId);
    }

    @GetMapping(path = "/chat")
    public Flux<String> chat(
            @RequestParam("userId") long userId,
            @RequestParam("sessionId") long sessionId,
            @RequestParam("taskId") long taskId,
            @RequestParam("postId") long postId,
            @RequestParam("version") Integer version,
            @RequestParam("question") String question,
            @RequestParam(value = "topK", defaultValue = "5") int topK,
            @RequestParam(value = "maxTokens", defaultValue = "1024") int maxTokens) {
        return chatService.chat(userId, sessionId, taskId, question, postId, version, topK, maxTokens);
    }
}
