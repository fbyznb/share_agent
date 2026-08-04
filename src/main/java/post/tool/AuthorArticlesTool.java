package post.tool;

import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;
import post.mapper.PostMapper;

import java.util.List;

@Component
public class AuthorArticlesTool {

    public static final String AUTHOR_ID_CONTEXT_KEY = "articleAuthorId";

    private final PostMapper postMapper;

    public AuthorArticlesTool(PostMapper postMapper) {
        this.postMapper = postMapper;
    }

    @Tool(
            name = "get_author_articles",
            description = "查询当前文章作者发布的所有文章标题，仅在用户询问该作者的其他文章或作品列表时调用"
    )
    public List<String> getAuthorArticles(ToolContext toolContext) {
        Object authorId = toolContext.getContext().get(AUTHOR_ID_CONTEXT_KEY);
        if (!(authorId instanceof Number number)) {
            throw new IllegalArgumentException("缺少工具上下文: " + AUTHOR_ID_CONTEXT_KEY);
        }
        return postMapper.selectTitlesByUserId(number.longValue());
    }
}
