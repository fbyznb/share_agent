package sku.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import sku.model.OrderOutcome;
import sku.service.OrderPersistenceService;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class OrderOutcomeInitializationTest {

    @Test
    void initializesOutcomeTableBeforeRecoveryQueriesUsingApplicationConfiguration() {
        contextRunner(databaseUrl()).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getEnvironment().getProperty("spring.sql.init.mode"))
                    .isEqualTo("always");
            assertThat(context.getEnvironment().getProperty("spring.sql.init.schema-locations"))
                    .isEqualTo("classpath:db/order-outcome.sql");

            OrderPersistenceService persistence = context.getBean(OrderPersistenceService.class);
            assertThat(persistence.findPendingRedisOutcomes(10)).isEmpty();

            JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
            insertPendingOutcome(jdbc);
            assertThat(persistence.findPendingRedisOutcomes(10))
                    .containsExactly(new OrderOutcome(101L, 10L, 20L, true, true));
        });
    }

    @Test
    void restartsPreserveOutcomesAndExistingOrderUniqueIndex() {
        String url = databaseUrl();
        JdbcTemplate existingDatabase = new JdbcTemplate(new DriverManagerDataSource(url, "sa", ""));
        existingDatabase.execute("""
                CREATE TABLE `order` (
                    id BIGINT PRIMARY KEY,
                    sku_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL
                )
                """);
        existingDatabase.execute("CREATE UNIQUE INDEX uk_order_sku_user ON `order` (sku_id, user_id)");
        existingDatabase.update("INSERT INTO `order` (id, sku_id, user_id) VALUES (101, 10, 20)");

        ApplicationContextRunner runner = contextRunner(url);
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            insertPendingOutcome(context.getBean(JdbcTemplate.class));
        });

        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(OrderPersistenceService.class).findPendingRedisOutcomes(10))
                    .containsExactly(new OrderOutcome(101L, 10L, 20L, true, true));

            JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM `order` WHERE id = 101", Integer.class))
                    .isEqualTo(1);
            assertThatExceptionOfType(DuplicateKeyException.class)
                    .isThrownBy(() -> jdbc.update(
                            "INSERT INTO `order` (id, sku_id, user_id) VALUES (102, 10, 20)"));
        });
    }

    private static ApplicationContextRunner contextRunner(String url) {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        JdbcTemplateAutoConfiguration.class,
                        SqlInitializationAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(OrderPersistenceService.class)
                .withPropertyValues(
                        "spring.config.location=classpath:application.yaml",
                        "spring.datasource.url=" + url,
                        "spring.datasource.driver-class-name=org.h2.Driver",
                        "spring.datasource.username=sa",
                        "spring.datasource.password=",
                        "spring.datasource.hikari.minimum-idle=0",
                        "spring.datasource.hikari.maximum-pool-size=1");
    }

    private static String databaseUrl() {
        return "jdbc:h2:mem:order_outcome_initialization_" + UUID.randomUUID()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1";
    }

    private static void insertPendingOutcome(JdbcTemplate jdbc) {
        jdbc.update("""
                INSERT INTO order_outcome
                    (order_id, sku_id, user_id, status, keep_buyer, next_retry_at)
                VALUES (101, 10, 20, 'CONFIRMED', TRUE, '2000-01-01 00:00:00')
                """);
    }
}
