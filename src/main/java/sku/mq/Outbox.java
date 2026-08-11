package sku.mq;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

/**
 * 待发送的 MQ 消息。
 */
@Entity
@Table(
        name = "outbox",
        indexes = @Index(
                name = "idx_status_retry",
                columnList = "status, retry_count"
        )
)
public class Outbox {

    @Id
    @Column(name = "event_id", nullable = false)
    private Long eventId;

    @Column(name = "payload", nullable = false, columnDefinition = "JSON")
    private String payload;

    @Column(name = "status", nullable = false)
    private Integer status = 0;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "event_type", nullable = false, length = 128)
    private String eventType;

    public Outbox() {
    }

    public Outbox(Long eventId, String payload, String eventType) {
        this.eventId = eventId;
        this.payload = payload;
        this.eventType = eventType;
    }

    public Long getEventId() {
        return eventId;
    }

    public void setEventId(Long eventId) {
        this.eventId = eventId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "Outbox{" +
                "eventId=" + eventId +
                ", payload='" + payload + '\'' +
                ", status=" + status +
                ", retryCount=" + retryCount +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
