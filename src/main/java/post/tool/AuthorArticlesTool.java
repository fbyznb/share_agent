package post.tool;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;
import post.mapper.PostMapper;

import java.util.List;

@Component
public class AuthorArticlesTool {

    private final PostMapper postMapper;

    public AuthorArticlesTool(PostMapper postMapper) {
        this.postMapper = postMapper;
    }

    @Tool(
            name = "get_author_articles",
            description = "根据用户 ID 查询该作者的所有文章标题"
    )
    public List<String> getAuthorArticles(
            @ToolParam(description = "作者的用户 ID") Long userId
    ) {
        if (userId == null) {
            throw new IllegalArgumentException("userId 不能为空");
        }
        return postMapper.selectTitlesByUserId(userId);
    }
}
