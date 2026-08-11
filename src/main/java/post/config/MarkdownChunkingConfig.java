package post.config;

import java.util.List;

import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.parser.IncludeSourceSpans;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.text.TextContentRenderer;
import org.springframework.ai.tokenizer.JTokkitTokenCountEstimator;
import org.springframework.ai.tokenizer.TokenCountEstimator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MarkdownChunkingConfig {

    private static final List<Extension> MARKDOWN_EXTENSIONS = List.of(
            TablesExtension.create()
    );

    @Bean
    public Parser markdownParser() {
        return Parser.builder()
                .extensions(MARKDOWN_EXTENSIONS)
                .includeSourceSpans(IncludeSourceSpans.BLOCKS)
                .build();
    }

    @Bean
    public TextContentRenderer markdownTextRenderer() {
        return TextContentRenderer.builder()
                .extensions(MARKDOWN_EXTENSIONS)
                .build();
    }

    @Bean
    public TokenCountEstimator markdownTokenCountEstimator() {
        return new JTokkitTokenCountEstimator();
    }
}
