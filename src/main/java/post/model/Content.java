package post.model;

import jakarta.persistence.*;

@Entity
@Table(name = "contents")
public class Content {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "content_id")
    private Long contentId;

    @Column(name = "post_id", nullable = false)
    private Long postId;

    @Column(name = "version", nullable = false)
    private Integer version;

    @Column(name = "content", nullable = false, columnDefinition = "LONGTEXT")
    private String content;

    public Content() {
    }

    public Content(Long postId, Integer version, String content) {
        this.postId = postId;
        this.version = version;
        this.content = content;
    }

    public Long getContentId() {
        return contentId;
    }

    public void setContentId(Long contentId) {
        this.contentId = contentId;
    }

    public Long getPostId() {
        return postId;
    }

    public void setPostId(Long postId) {
        this.postId = postId;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Content{" +
                "contentId=" + contentId +
                ", postId=" + postId +
                ", version=" + version +
                ", content='" + content + '\'' +
                '}';
    }
}