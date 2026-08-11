package sku.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

/**
 * SKU 收藏记录。
 */
@Entity
@Table(
        name = "fav",
        indexes = {
                @Index(name = "idx_sku_id", columnList = "sku_id"),
                @Index(name = "idx_user_id", columnList = "user_id"),
                @Index(
                        name = "idx_sku_user",
                        columnList = "sku_id, user_id",
                        unique = true
                )
        }
)
public class Fav {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "sku_id", nullable = false)
    private Long skuId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "status", nullable = false)
    private Integer status;

    public Fav() {
    }

    public Fav(Long skuId, Long userId, Integer status) {
        this.skuId = skuId;
        this.userId = userId;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSkuId() {
        return skuId;
    }

    public void setSkuId(Long skuId) {
        this.skuId = skuId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Fav{" +
                "id=" + id +
                ", skuId=" + skuId +
                ", userId=" + userId +
                ", status=" + status +
                '}';
    }
}
