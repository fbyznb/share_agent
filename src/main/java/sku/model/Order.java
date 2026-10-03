package sku.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

/**
 * 订单。
 */
@Entity
@Table(
        name = "`order`",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_order_sku_user", columnNames = {"sku_id", "user_id"}
        ),
        indexes = {
                @Index(name = "idx_sku_id", columnList = "sku_id"),
                @Index(name = "idx_user_id", columnList = "user_id")
        }
)
public class Order {

    @Id
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "sku_id", nullable = false)
    private Long skuId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    public Order() {
    }

    public Order(Long id, Long skuId, Long userId) {
        this.id = id;
        this.skuId = skuId;
        this.userId = userId;
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

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", skuId=" + skuId +
                ", userId=" + userId +
                '}';
    }
}
