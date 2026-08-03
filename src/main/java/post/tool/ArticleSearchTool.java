package post.tool;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.alibaba.cloud.ai.dashscope.rerank.DashScopeRerankOptions;
import com.alibaba.cloud.ai.model.RerankModel;
import com.alibaba.cloud.ai.model.RerankRequest;
import org.springframework.ai.document.Document;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ArticleSearchTool {

    private final VectorStore vectorStore;
    private final ElasticsearchClient elasticsearchClient;
    private final RerankModel rerankModel;
    private final String indexName;

    public ArticleSearchTool(
            VectorStore vectorStore,
            ElasticsearchClient elasticsearchClient,
            RerankModel rerankModel,
            @Value("${spring.ai.vectorstore.elasticsearch.index-name}") String indexName
    ) {
        this.vectorStore = vectorStore;
        this.elasticsearchClient = elasticsearchClient;
        this.rerankModel = rerankModel;
        this.indexName = indexName;
    }

    @Tool(
            name = "search_article_chunks",
            description = "在指定文章版本中进行语义与关键词混合检索，返回重排后的相关原文片段"
    )
    @SuppressWarnings("rawtypes")
    public List<String> searchArticleChunks(
            @ToolParam(description = "文章 ID") Long postId,
            @ToolParam(description = "文章版本号") Integer version,
            @ToolParam(description = "用于向量检索的完整语义查询") String semanticQuery,
            @ToolParam(description = "用于 BM25 全文检索的关键词，多个关键词用空格分隔") String keywords
    ) {
        validateArguments(postId, version, semanticQuery, keywords);

        FilterExpressionBuilder filterBuilder = new FilterExpressionBuilder();
        List<Document> docs = vectorStore.similaritySearch(
                SearchRequest.builder()
                        .query(semanticQuery)
                        .topK(40)
                        .filterExpression(
                                filterBuilder.and(
                                        filterBuilder.eq("postId", postId),
                                        filterBuilder.eq("version", version)
                                ).build()
                        )
                        .build()
        );

        SearchResponse<Map> response;
        try {
            response = elasticsearchClient.search(
                    search -> search
                            .index(indexName)
                            .size(40)
                            .query(queryBuilder -> queryBuilder
                                    .bool(bool -> bool
                                            .must(must -> must
                                                    .match(match -> match
                                                            .field("content")
                                                            .query(keywords)
                                                    )
                                            )
                                            .filter(filter -> filter
                                                    .term(term -> term
                                                            .field("metadata.postId")
                                                            .value(FieldValue.of(postId))
                                                    )
                                            )
                                            .filter(filter -> filter
                                                    .term(term -> term
                                                            .field("metadata.version")
                                                            .value(FieldValue.of(version.longValue()))
                                                    )
                                            )
                                    )
                            ),
                    Map.class
            );
        } catch (IOException exception) {
            throw new IllegalStateException("文章 BM25 检索失败", exception);
        }

        final int rrfK = 60;
        Map<String, Double> rrfScores = new HashMap<>();
        Map<String, Document> chunksById = new HashMap<>();

        for (int i = 0; i < docs.size(); i++) {
            Document document = docs.get(i);
            Object chunkIdValue = document.getMetadata().get("chunkId");
            if (chunkIdValue == null) {
                continue;
            }

            String chunkId = String.valueOf(chunkIdValue);
            chunksById.putIfAbsent(chunkId, document);
            rrfScores.merge(chunkId, 1.0 / (rrfK + i + 1), Double::sum);
        }

        for (int i = 0; i < response.hits().hits().size(); i++) {
            Map<?, ?> source = response.hits().hits().get(i).source();
            if (source == null || !(source.get("metadata") instanceof Map<?, ?> rawMetadata)) {
                continue;
            }

            Object chunkIdValue = rawMetadata.get("chunkId");
            Object contentValue = source.get("content");
            if (chunkIdValue == null || contentValue == null) {
                continue;
            }

            String chunkId = String.valueOf(chunkIdValue);
            if (!chunksById.containsKey(chunkId)) {
                Map<String, Object> metadata = new HashMap<>();
                rawMetadata.forEach((key, value) -> {
                    if (key != null) {
                        metadata.put(String.valueOf(key), value);
                    }
                });
                chunksById.put(chunkId, new Document(String.valueOf(contentValue), metadata));
            }
            rrfScores.merge(chunkId, 1.0 / (rrfK + i + 1), Double::sum);
        }

        List<Document> rrfResults = rrfScores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(30)
                .map(entry -> chunksById.get(entry.getKey()))
                .toList();

        List<Document> rerankedResults;
        if (rrfResults.isEmpty()) {
            rerankedResults = List.of();
        } else {
            RerankRequest rerankRequest = new RerankRequest(
                    semanticQuery,
                    rrfResults,
                    DashScopeRerankOptions.builder()
                            .model("gte-rerank-v2")
                            .topN(5)
                            .returnDocuments(true)
                            .build()
            );
            rerankedResults = rerankModel.call(rerankRequest)
                    .getResults()
                    .stream()
                    .map(result -> result.getOutput())
                    .filter(document -> document != null)
                    .limit(5)
                    .toList();
        }

        Map<Integer, Document> selectedChunksByIndex = new HashMap<>();
        for (Document document : rerankedResults) {
            Integer chunkIndex = parseChunkIndex(document);
            if (chunkIndex != null) {
                selectedChunksByIndex.put(chunkIndex, document);
            }
        }

        List<String> results = new ArrayList<>(rerankedResults.size());
        for (Document document : rerankedResults) {
            String text = document.getText();
            Integer chunkIndex = parseChunkIndex(document);
            if (chunkIndex != null) {
                Document previous = selectedChunksByIndex.get(chunkIndex - 1);
                if (previous != null) {
                    text = removePrefixOverlap(previous.getText(), text, 100);
                }
            }
            results.add(text);
        }
        return results;
    }

    private static void validateArguments(
            Long postId,
            Integer version,
            String semanticQuery,
            String keywords
    ) {
        if (postId == null) {
            throw new IllegalArgumentException("postId 不能为空");
        }
        if (version == null) {
            throw new IllegalArgumentException("version 不能为空");
        }
        if (semanticQuery == null || semanticQuery.isBlank()) {
            throw new IllegalArgumentException("semanticQuery 不能为空");
        }
        if (keywords == null || keywords.isBlank()) {
            throw new IllegalArgumentException("keywords 不能为空");
        }
    }

    private static Integer parseChunkIndex(Document document) {
        Object chunkIdValue = document.getMetadata().get("chunkId");
        if (chunkIdValue == null) {
            return null;
        }

        String chunkId = String.valueOf(chunkIdValue);
        int separator = chunkId.lastIndexOf('#');
        if (separator < 0 || separator == chunkId.length() - 1) {
            return null;
        }

        try {
            return Integer.valueOf(chunkId.substring(separator + 1));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String removePrefixOverlap(String previous, String current, int maxOverlap) {
        if (previous == null || current == null || previous.isEmpty() || current.isEmpty()) {
            return current;
        }

        int upperBound = Math.min(maxOverlap, Math.min(previous.length(), current.length()));
        for (int length = upperBound; length > 0; length--) {
            if (previous.regionMatches(previous.length() - length, current, 0, length)) {
                return current.substring(length);
            }
        }
        return current;
    }
}
