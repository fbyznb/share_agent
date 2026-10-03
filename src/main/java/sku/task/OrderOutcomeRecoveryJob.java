package sku.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import sku.model.OrderOutcome;
import sku.mq.OrderReservationStore;
import sku.service.OrderPersistenceService;

/** Retries committed decisions independently of RocketMQ's finite delivery retry count. */
@Component
public class OrderOutcomeRecoveryJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderOutcomeRecoveryJob.class);
    private final OrderPersistenceService persistence;
    private final OrderReservationStore reservations;
    private final int batchSize;
    private final int retryDelaySeconds;

    public OrderOutcomeRecoveryJob(
            OrderPersistenceService persistence,
            OrderReservationStore reservations,
            @Value("${sku.order.outcome-recovery.batch-size:100}") int batchSize,
            @Value("${sku.order.outcome-recovery.retry-delay-seconds:30}") int retryDelaySeconds
    ) {
        if (batchSize < 1 || retryDelaySeconds < 1) {
            throw new IllegalArgumentException("Outcome recovery batch size and retry delay must be positive");
        }
        this.persistence = persistence;
        this.reservations = reservations;
        this.batchSize = batchSize;
        this.retryDelaySeconds = retryDelaySeconds;
    }

    @Scheduled(fixedDelayString = "${sku.order.outcome-recovery.interval-ms:5000}")
    public void recover() {
        // Multiple instances may select the same rows; the Redis terminal transition is idempotent.
        for (OrderOutcome outcome : persistence.findPendingRedisOutcomes(batchSize)) {
            try {
                reservations.complete(outcome.order(), outcome.accepted(), outcome.keepBuyer());
                persistence.markRedisApplied(outcome.orderId());
            } catch (RuntimeException ex) {
                LOGGER.error("Failed to complete Redis reservation for order {}", outcome.orderId(), ex);
                try {
                    // Broken records must not monopolize the first page of every scheduled batch.
                    persistence.postponeRedisCompletion(outcome.orderId(), retryDelaySeconds);
                } catch (RuntimeException postponeFailure) {
                    LOGGER.error("Failed to postpone reservation retry for order {}",
                            outcome.orderId(), postponeFailure);
                }
            }
        }
    }
}
