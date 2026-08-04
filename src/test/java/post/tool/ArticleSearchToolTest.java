package post.tool;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.alibaba.cloud.ai.model.RerankModel;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.vectorstore.VectorStore;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ArticleSearchToolTest {

    @Test
    void rejectsMissingRequestScopeContextBeforeSearching() {
        ArticleSearchTool tool = new ArticleSearchTool(
                mock(VectorStore.class),
                mock(ElasticsearchClient.class),
                mock(RerankModel.class),
                mock(ChatClient.class),
                "test-index"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> tool.searchArticleChunks("缺失的文章证据", new ToolContext(Map.of()))
        );
    }

    @Test
    void rejectsBlankEvidenceQuery() {
        ArticleSearchTool tool = new ArticleSearchTool(
                mock(VectorStore.class),
                mock(ElasticsearchClient.class),
                mock(RerankModel.class),
                mock(ChatClient.class),
                "test-index"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> tool.searchArticleChunks(
                        " ",
                        new ToolContext(Map.of(
                                ArticleSearchTool.POST_ID_CONTEXT_KEY, 1L,
                                ArticleSearchTool.VERSION_CONTEXT_KEY, 1
                        ))
                )
        );
    }
}
