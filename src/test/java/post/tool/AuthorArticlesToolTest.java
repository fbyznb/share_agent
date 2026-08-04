package post.tool;

import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ToolContext;
import post.mapper.PostMapper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuthorArticlesToolTest {

    @Test
    void usesAuthorIdFromToolContext() {
        PostMapper postMapper = mock(PostMapper.class);
        when(postMapper.selectTitlesByUserId(42L)).thenReturn(List.of("文章一", "文章二"));
        AuthorArticlesTool tool = new AuthorArticlesTool(postMapper);

        List<String> result = tool.getAuthorArticles(
                new ToolContext(Map.of(AuthorArticlesTool.AUTHOR_ID_CONTEXT_KEY, 42L))
        );

        assertEquals(List.of("文章一", "文章二"), result);
        verify(postMapper).selectTitlesByUserId(42L);
    }

    @Test
    void rejectsMissingAuthorContext() {
        AuthorArticlesTool tool = new AuthorArticlesTool(mock(PostMapper.class));

        assertThrows(
                IllegalArgumentException.class,
                () -> tool.getAuthorArticles(new ToolContext(Map.of()))
        );
    }
}
