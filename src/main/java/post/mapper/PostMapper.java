package post.mapper;

import post.model.Post;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface PostMapper {
        void insert(Post post);
        Post select(Long postId);
        List<String> selectTitlesByUserId(@Param("userId") Long userId);
        Integer update(Long postId,Integer version,Integer status);
        Integer publishVersion(Long postId,Integer version);
        Post selectVersionAndStatus(Long postId);
}