package post.service;

import post.controller.dto.UpdateResponse;
import post.controller.dto.InsertResponse;
import post.controller.dto.SelectResponse;

public interface PostService {
    InsertResponse insertPost(Long userId,String title);
    UpdateResponse selectContentByPost(Long postId);
    boolean publishContent(String content, Long postId, Integer version, Integer status);
    SelectResponse selectPost(Long postId);
}
