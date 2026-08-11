package sku.mq;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CanalRocketMQBridgeTest {

    private final RocketMQTemplate rocketMQTemplate =
            mock(RocketMQTemplate.class);
    private final CanalRocketMQBridge bridge = new CanalRocketMQBridge(
            rocketMQTemplate,
            "127.0.0.1",
            11111,
            "example",
            "",
            "",
            "share_agent\\.outbox",
            100,
            1000L,
            2000L
    );

    @Test
    void acknowledgesDeleteInsteadOfRollingBackForever() throws Exception {
        CanalConnector connector = connectorReturning(
                new Message(7L, List.of(entry(CanalEntry.EventType.DELETE)))
        );

        bridge.consumeNextBatch(connector);

        verify(connector).ack(7L);
        verify(connector, never()).rollback(anyLong());
        verify(rocketMQTemplate, never()).syncSend(
                any(String.class),
                any(Outbox.class)
        );
    }

    @Test
    void skipsDeleteAndPublishesFollowingInsert() throws Exception {
        SendResult sendResult = mock(SendResult.class);
        when(sendResult.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(rocketMQTemplate.syncSend(
                eq("canal_outbox"),
                any(Outbox.class)
        )).thenReturn(sendResult);
        CanalConnector connector = connectorReturning(new Message(
                8L,
                List.of(
                        entry(CanalEntry.EventType.DELETE),
                        entry(CanalEntry.EventType.INSERT)
                )
        ));

        bridge.consumeNextBatch(connector);

        ArgumentCaptor<Outbox> event = ArgumentCaptor.forClass(Outbox.class);
        verify(rocketMQTemplate).syncSend(eq("canal_outbox"), event.capture());
        assertEquals(101L, event.getValue().getEventId());
        assertEquals("CounterEvent", event.getValue().getEventType());
        verify(connector).ack(8L);
        verify(connector, never()).rollback(anyLong());
    }

    @Test
    void rollsBackWhenRocketMqDoesNotConfirmSend() throws Exception {
        SendResult sendResult = mock(SendResult.class);
        when(sendResult.getSendStatus())
                .thenReturn(SendStatus.FLUSH_DISK_TIMEOUT);
        when(rocketMQTemplate.syncSend(
                eq("canal_outbox"),
                any(Outbox.class)
        )).thenReturn(sendResult);
        CanalConnector connector = connectorReturning(
                new Message(9L, List.of(entry(CanalEntry.EventType.INSERT)))
        );

        assertThrows(
                IllegalStateException.class,
                () -> bridge.consumeNextBatch(connector)
        );

        verify(connector).rollback(9L);
        verify(connector, never()).ack(anyLong());
    }

    private CanalConnector connectorReturning(Message message) throws Exception {
        CanalConnector connector = mock(CanalConnector.class);
        when(connector.getWithoutAck(
                100,
                1000L,
                TimeUnit.MILLISECONDS
        )).thenReturn(message);
        return connector;
    }

    private CanalEntry.Entry entry(CanalEntry.EventType eventType) {
        CanalEntry.RowData.Builder row = CanalEntry.RowData.newBuilder();
        if (eventType == CanalEntry.EventType.INSERT) {
            row.addAfterColumns(column("event_id", "101"));
            row.addAfterColumns(column("payload", "{\"sid\":1}"));
            row.addAfterColumns(column("status", "0"));
            row.addAfterColumns(column("retry_count", "0"));
            row.addAfterColumns(column("event_type", "CounterEvent"));
        }

        CanalEntry.RowChange change = CanalEntry.RowChange.newBuilder()
                .setEventType(eventType)
                .addRowDatas(row)
                .build();
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .setSchemaName("share_agent")
                .setTableName("outbox")
                .build();
        return CanalEntry.Entry.newBuilder()
                .setHeader(header)
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setStoreValue(change.toByteString())
                .build();
    }

    private CanalEntry.Column column(String name, String value) {
        return CanalEntry.Column.newBuilder()
                .setName(name)
                .setValue(value)
                .build();
    }
}
