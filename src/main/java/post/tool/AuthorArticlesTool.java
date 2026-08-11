package post.tool;

import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;
import post.mapper.PostMapper;

import java.util.List;

@Component
public class AuthorArticlesTool {

    public static final String TOOL_NAME = "get_author_articles";
    public static final String AUTHOR_ID_CONTEXT_KEY = "articleAuthorId";

    private final PostMapper postMapper;

    public AuthorArticlesTool(PostMapper postMapper) {
        this.postMapper = postMapper;
    }

    @Tool(
            name = TOOL_NAME,
            description = "查询当前文章作者发布的所有文章标题，用于回答该作者的文章或作品列表、其他作品、发布数量或总数。"
                    + "作者已由后端根据当前 postId 确定，调用时无需传入作者 ID，也不应询问用户作者是谁"
    )
    public List<String> getAuthorArticles(ToolContext toolContext) {
        Object authorId = toolContext.getContext().get(AUTHOR_ID_CONTEXT_KEY);
        if (!(authorId instanceof Number number)) {
            throw new IllegalArgumentException("缺少工具上下文: " + AUTHOR_ID_CONTEXT_KEY);
        }
        return postMapper.selectTitlesByUserId(number.longValue());
    }
}
