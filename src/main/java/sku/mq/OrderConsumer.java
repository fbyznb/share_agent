package sku.mq;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;
import sku.model.Order;
import sku.model.OrderOutcome;
import sku.service.OrderPersistenceService;

@Component
@RocketMQMessageListener(
        topic = "order",
        consumerGroup = "order_consumer_group",
        selectorExpression = "*",
        messageModel = MessageModel.CLUSTERING,
        consumeMode = ConsumeMode.CONCURRENTLY
)
public class OrderConsumer implements RocketMQListener<Order> {

    private final OrderPersistenceService persistence;
    private final OrderReservationStore reservations;

    public OrderConsumer(OrderPersistenceService persistence, OrderReservationStore reservations) {
        this.persistence = persistence;
        this.reservations = reservations;
    }

    @Override
    public void onMessage(Order order) {
        // The proxied service commits first. Redis failure must not undo the MySQL decision.
        OrderOutcome outcome = persistence.persist(order);
        reservations.complete(outcome.order(), outcome.accepted(), outcome.keepBuyer());
        persistence.markRedisApplied(outcome.orderId());
        // Exceptions propagate for MQ redelivery; recovery also covers exhausted Redis retries.
    }
}
