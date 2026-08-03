package post.mapper;



import post.model.Content;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ContentMapper {
    void insert(Content content);
    Content selectContentByPost(Long postId,Integer version);
    Integer update(Long postId,Integer version,String content);
}
