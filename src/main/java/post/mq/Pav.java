package post.mq;

public class Pav {
    public Long postId;
    public Integer version;

    public Pav() {
    }

    public Pav(Long p, Integer v) {
        postId = p;
        version = v;
    }
}
