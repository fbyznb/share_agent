package post.memory;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import post.model.Message;

/**
 * 使用 chat 和 message 表持久化会话消息的仓库。
 *
 * <p>message 表只做追加写入，历史消息不会在摘要窗口滚动时被删除。
 * chat.last_message_id 标识已被 chat.summary 覆盖的最后一条消息。</p>
 */
@Repository
public class DatabaseChatMemoryRepository {

    private final JdbcTemplate jdbcTemplate;

    public DatabaseChatMemoryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<String> findConversationIds() {
        return this.jdbcTemplate.query(
                "select id from `chat` order by id",
                (resultSet, rowNum) -> String.valueOf(resultSet.getLong("id"))
        );
    }

    /**
     * 返回尚未被摘要覆盖的原始消息。摘要由 SummarizingWindowChatMemory 注入上下文，
     * 不伪装成 message 表中的消息。
     */
    public List<Message> findByConversationId(String conversationId) {
        return loadConversation(conversationId).messages().stream()
                .map(StoredMessage::message)
                .toList();
    }

    /**
     * 将新消息追加到 message 表。
     */
    @Transactional
    public void saveAll(String conversationId, List<Message> messages) {
        append(conversationId, messages);
    }

    @Transactional
    public void deleteByConversationId(String conversationId) {
        long chatId = parseConversationId(conversationId);
        this.jdbcTemplate.update(
                "delete from `message` where conversation_id = ?",
                chatId
        );
        this.jdbcTemplate.update(
                "update `chat` set summary = null, last_message_id = null where id = ?",
                chatId
        );
    }

    @Transactional(readOnly = true)
    public ConversationState loadConversation(String conversationId) {
        long chatId = parseConversationId(conversationId);
        List<ConversationHeader> headers = this.jdbcTemplate.query(
                "select summary, last_message_id from `chat` where id = ?",
                (resultSet, rowNum) -> new ConversationHeader(
                        resultSet.getString("summary"),
                        resultSet.getObject("last_message_id", Long.class)
                ),
                chatId
        );
        if (headers.isEmpty()) {
            throw new IllegalArgumentException("chat does not exist: " + conversationId);
        }

        ConversationHeader header = headers.get(0);
        List<StoredMessage> messages = this.jdbcTemplate.query(
                """
                select id, conversation_id, version, role, content, created_at
                from `message`
                where conversation_id = ?
                  and (? is null or id > ?)
                order by id
                """,
                (resultSet, rowNum) -> new StoredMessage(
                        resultSet.getLong("id"),
                        toModelMessage(resultSet)
                ),
                chatId,
                header.lastMessageId(),
                header.lastMessageId()
        );
        return new ConversationState(header.summary(), header.lastMessageId(), messages);
    }

    /**
     * 追加消息并返回数据库生成的消息 ID。
     */
    @Transactional
    public List<StoredMessage> append(String conversationId, List<Message> messages) {
        Assert.notNull(messages, "messages cannot be null");
        Assert.noNullElements(messages, "messages cannot contain null elements");

        long chatId = parseConversationId(conversationId);
        int defaultVersion = findChatPostVersion(chatId);
        List<StoredMessage> storedMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            this.jdbcTemplate.update(connection -> {
                PreparedStatement statement = connection.prepareStatement(
                        """
                        insert into `message` (conversation_id, version, role, content)
                        values (?, ?, ?, ?)
                        """,
                        Statement.RETURN_GENERATED_KEYS
                );
                statement.setLong(1, chatId);
                statement.setInt(
                        2,
                        message.getVersion() == null ? defaultVersion : message.getVersion()
                );
                statement.setString(3, message.getRole());
                statement.setString(
                        4,
                        message.getContent() == null ? "" : message.getContent()
                );
                return statement;
            }, keyHolder);

            Number generatedId = keyHolder.getKey();
            if (generatedId == null) {
                throw new IllegalStateException("database did not return a generated message id");
            }
            storedMessages.add(new StoredMessage(generatedId.longValue(), message));
        }
        return List.copyOf(storedMessages);
    }

    @Transactional
    public void updateSummary(
            String conversationId,
            String summary,
            long lastMessageId
    ) {
        long chatId = parseConversationId(conversationId);
        int updated = this.jdbcTemplate.update(
                """
                update `chat`
                set summary = ?, last_message_id = ?
                where id = ?
                """,
                summary,
                lastMessageId,
                chatId
        );
        if (updated != 1) {
            throw new IllegalArgumentException("chat does not exist: " + conversationId);
        }
    }

    private int findChatPostVersion(long chatId) {
        List<Integer> versions = this.jdbcTemplate.query(
                """
                select p.version
                from `chat` c
                join posts p on p.post_id = c.post_id
                where c.id = ?
                """,
                (resultSet, rowNum) -> resultSet.getInt("version"),
                chatId
        );
        if (versions.isEmpty()) {
            throw new IllegalArgumentException("chat does not exist: " + chatId);
        }
        return versions.get(0);
    }

    private static Message toModelMessage(java.sql.ResultSet resultSet)
            throws java.sql.SQLException {
        Message message = new Message();
        message.setId(resultSet.getLong("id"));
        message.setConversationId(resultSet.getLong("conversation_id"));
        message.setVersion(resultSet.getInt("version"));
        message.setRole(resultSet.getString("role"));
        message.setContent(resultSet.getString("content"));
        message.setCreatedAt(
                resultSet.getTimestamp("created_at").toLocalDateTime()
        );
        return message;
    }

    private static long parseConversationId(String conversationId) {
        Assert.hasText(conversationId, "conversationId cannot be null or empty");
        try {
            return Long.parseLong(conversationId);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "conversationId must be the numeric id of a chat",
                    exception
            );
        }
    }

    public record ConversationState(
            String summary,
            Long lastMessageId,
            List<StoredMessage> messages
    ) {
    }

    public record StoredMessage(Long id, Message message) {
    }

    private record ConversationHeader(String summary, Long lastMessageId) {
    }
}
