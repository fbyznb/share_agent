package post.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

/**
 * AI会话。
 */
@Entity
@Table(
        name = "chat",
        indexes = @Index(name = "idx_user_post", columnList = "user_id, post_id")
)
public class Chat {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "post_id", nullable = false)
    private Long postId;

    @Column(name = "summary", columnDefinition = "LONGTEXT")
    private String summary;

    @Column(name = "last_message_id")
    private Long lastMessageId;

    public Chat() {
    }

    public Chat(Long userId, Long postId, String summary, Long lastMessageId) {
        this.userId = userId;
        this.postId = postId;
        this.summary = summary;
        this.lastMessageId = lastMessageId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getPostId() {
        return postId;
    }

    public void setPostId(Long postId) {
        this.postId = postId;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public Long getLastMessageId() {
        return lastMessageId;
    }

    public void setLastMessageId(Long lastMessageId) {
        this.lastMessageId = lastMessageId;
    }

    @Override
    public String toString() {
        return "Chat{" +
                "id=" + id +
                ", userId=" + userId +
                ", postId=" + postId +
                ", summary='" + summary + '\'' +
                ", lastMessageId=" + lastMessageId +
                '}';
    }
}
