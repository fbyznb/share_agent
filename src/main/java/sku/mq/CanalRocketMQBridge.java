package sku.mq;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 将 Canal 捕获的 outbox 行变更转发到 RocketMQ。
 */
@Component
public class CanalRocketMQBridge implements SmartLifecycle {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanalRocketMQBridge.class);
    private static final String ROCKETMQ_TOPIC = "canal_outbox";

    private final RocketMQTemplate rocketMQTemplate;
    private final String host;
    private final int port;
    private final String destination;
    private final String username;
    private final String password;
    private final String subscription;
    private final int batchSize;
    private final long pollTimeoutMillis;
    private final long reconnectDelayMillis;

    private volatile boolean running;
    private volatile CanalConnector connector;
    private Thread worker;

    public CanalRocketMQBridge(
            RocketMQTemplate rocketMQTemplate,
            @Value("${canal.bridge.host:127.0.0.1}") String host,
            @Value("${canal.bridge.port:11111}") int port,
            @Value("${canal.bridge.destination:example}") String destination,
            @Value("${canal.bridge.username:}") String username,
            @Value("${canal.bridge.password:}") String password,
            @Value("${canal.bridge.subscription:share_agent\\.outbox}")
            String subscription,
            @Value("${canal.bridge.batch-size:100}") int batchSize,
            @Value("${canal.bridge.poll-timeout-millis:1000}")
            long pollTimeoutMillis,
            @Value("${canal.bridge.reconnect-delay-millis:2000}")
            long reconnectDelayMillis
    ) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.host = host;
        this.port = port;
        this.destination = destination;
        this.username = username;
        this.password = password;
        this.subscription = subscription;
        this.batchSize = batchSize;
        this.pollTimeoutMillis = pollTimeoutMillis;
        this.reconnectDelayMillis = reconnectDelayMillis;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        worker = new Thread(this::runBridge, "canal-outbox-bridge");
        worker.setDaemon(true);
        worker.start();
    }

    private void runBridge() {
        while (running) {
            CanalConnector current = createConnector();
            connector = current;
            try {
                current.connect();
                current.subscribe(subscription);
                current.rollback();
                LOGGER.info(
                        "Canal outbox bridge connected to {}:{} destination {}",
                        host,
                        port,
                        destination
                );
                consume(current);
            } catch (Exception ex) {
                if (running) {
                    LOGGER.error("Canal outbox bridge failed; reconnecting", ex);
                }
            } finally {
                closeConnector(current);
                if (connector == current) {
                    connector = null;
                }
            }

            if (running) {
                sleepBeforeReconnect();
            }
        }
    }

    private CanalConnector createConnector() {
        return CanalConnectors.newSingleConnector(
                new InetSocketAddress(host, port),
                destination,
                username,
                password
        );
    }

    private void consume(CanalConnector current) throws Exception {
        while (running) {
            consumeNextBatch(current);
        }
    }

    void consumeNextBatch(CanalConnector current) throws Exception {
        Message message = current.getWithoutAck(
                batchSize,
                pollTimeoutMillis,
                TimeUnit.MILLISECONDS
        );
        long batchId = message.getId();
        if (batchId == -1L) {
            return;
        }

        try {
            processBatch(message);
            current.ack(batchId);
        } catch (Exception ex) {
            current.rollback(batchId);
            throw ex;
        }
    }

    private void processBatch(Message message) throws Exception {
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType()
                    == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA
                    || !"outbox".equalsIgnoreCase(
                    entry.getHeader().getTableName()
            )) {
                continue;
            }

            CanalEntry.RowChange change =
                    CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            if (change.getIsDdl()
                    || change.getEventType() != CanalEntry.EventType.INSERT) {
                LOGGER.debug(
                        "Skip Canal {} event for {}.{}",
                        change.getEventType(),
                        entry.getHeader().getSchemaName(),
                        entry.getHeader().getTableName()
                );
                continue;
            }

            for (CanalEntry.RowData rowData : change.getRowDatasList()) {
                Outbox outbox = toOutbox(rowData);
                SendResult result = rocketMQTemplate.syncSend(
                        ROCKETMQ_TOPIC,
                        outbox
                );
                if (result == null
                        || result.getSendStatus() != SendStatus.SEND_OK) {
                    throw new IllegalStateException(
                            "RocketMQ did not confirm outbox event "
                                    + outbox.getEventId()
                    );
                }
            }
        }
    }

    private Outbox toOutbox(CanalEntry.RowData rowData) {
        Map<String, String> values = new HashMap<>();
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            values.put(
                    column.getName().toLowerCase(),
                    column.getIsNull() ? null : column.getValue()
            );
        }

        Outbox outbox = new Outbox();
        outbox.setEventId(Long.valueOf(required(values, "event_id")));
        outbox.setPayload(required(values, "payload"));
        outbox.setStatus(Integer.valueOf(required(values, "status")));
        outbox.setRetryCount(
                Integer.valueOf(required(values, "retry_count"))
        );
        outbox.setEventType(required(values, "event_type"));
        return outbox;
    }

    private String required(Map<String, String> values, String column) {
        String value = values.get(column);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Missing required outbox column: " + column
            );
        }
        return value;
    }

    private void sleepBeforeReconnect() {
        try {
            Thread.sleep(reconnectDelayMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        Thread currentWorker = worker;
        if (currentWorker != null) {
            currentWorker.interrupt();
        }
        closeConnector(connector);
        connector = null;

        if (currentWorker != null
                && currentWorker != Thread.currentThread()) {
            try {
                currentWorker.join(pollTimeoutMillis + 1000L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        worker = null;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    private void closeConnector(CanalConnector current) {
        if (current == null) {
            return;
        }
        try {
            current.unsubscribe();
        } catch (RuntimeException ex) {
            LOGGER.debug("Failed to unsubscribe Canal connector", ex);
        }
        try {
            current.disconnect();
        } catch (RuntimeException ex) {
            LOGGER.debug("Failed to disconnect Canal connector", ex);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
