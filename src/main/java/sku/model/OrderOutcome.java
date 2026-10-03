package sku.model;

/** A committed terminal decision; copied identifiers cannot be mutated by callers. */
public record OrderOutcome(long orderId, long skuId, long userId, boolean accepted, boolean keepBuyer) {
    public OrderOutcome(Order order, boolean accepted, boolean keepBuyer) {
        this(order.getId(), order.getSkuId(), order.getUserId(), accepted, keepBuyer);
    }

    public Order order() {
        return new Order(orderId, skuId, userId);
    }
}
