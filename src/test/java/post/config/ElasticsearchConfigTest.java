package post.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class ElasticsearchConfigTest {

    @Test
    void usesSpringElasticsearchUrisInsteadOfLegacyHostDefaults() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ElasticsearchRestClientAutoConfiguration.class,
                        ElasticsearchClientAutoConfiguration.class))
                .withUserConfiguration(ElasticsearchConfig.class)
                .withPropertyValues(
                        "spring.elasticsearch.uris=http://es-one.invalid:19200,https://es-two.invalid:19201",
                        "elasticsearch.host=legacy.invalid",
                        "elasticsearch.port=9200")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(RestClient.class);
                    assertThat(context).hasSingleBean(ElasticsearchClient.class);
                    assertThat(context.getBean(RestClient.class).getNodes())
                            .extracting(node -> node.getHost().toURI())
                            .containsExactly("http://es-one.invalid:19200", "https://es-two.invalid:19201");
                });
    }
}
