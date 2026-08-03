package post.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * 用户长期记忆。
 */
@Entity
@Table(name = "long_memory")
public class LongMemory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "memory_key", nullable = false, length = 255)
    private String memoryKey;

    @Column(name = "content", nullable = false, columnDefinition = "TEXT")
    private String content;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "ACTIVE";

    @Version
    @Column(name = "version", nullable = false)
    private Integer version = 1;

    @Column(name = "source_message_id")
    private Long sourceMessageId;

    public LongMemory() {
    }

    public LongMemory(Long userId, String memoryKey,
                      String content, Long sourceMessageId) {
        this.userId = userId;
        this.memoryKey = memoryKey;
        this.content = content;
        this.sourceMessageId = sourceMessageId;
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

    public String getMemoryKey() {
        return memoryKey;
    }

    public void setMemoryKey(String memoryKey) {
        this.memoryKey = memoryKey;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Long getSourceMessageId() {
        return sourceMessageId;
    }

    public void setSourceMessageId(Long sourceMessageId) {
        this.sourceMessageId = sourceMessageId;
    }

    @Override
    public String toString() {
        return "LongMemory{" +
                "id=" + id +
                ", userId=" + userId +
                ", memoryKey='" + memoryKey + '\'' +
                ", content='" + content + '\'' +
                ", status='" + status + '\'' +
                ", version=" + version +
                ", sourceMessageId=" + sourceMessageId +
                '}';
    }
}
