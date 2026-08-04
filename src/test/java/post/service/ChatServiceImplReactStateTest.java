package post.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ChatServiceImplReactStateTest {

    @Test
    void canonicalizesEquivalentToolArgumentsForDeduplication() {
        ChatServiceImpl service = serviceWithObjectMapper();

        String compact = ReflectionTestUtils.invokeMethod(
                service,
                "canonicalToolCallKey",
                "search_article_chunks",
                "{\"query\":\"Spring AI\"}"
        );
        String formatted = ReflectionTestUtils.invokeMethod(
                service,
                "canonicalToolCallKey",
                "search_article_chunks",
                "{ \"query\" : \"Spring AI\" }"
        );

        assertEquals(compact, formatted);
    }

    @Test
    void parsesOnlyValidTerminalDecisionJson() {
        ChatServiceImpl service = serviceWithObjectMapper();

        Object decision = ReflectionTestUtils.invokeMethod(
                service,
                "parseTerminalDecision",
                "```json\n{\"next_action\":\"ASK_USER\",\"user_question\":\"请提供错误日志\"}\n```"
        );
        Object invalid = ReflectionTestUtils.invokeMethod(
                service,
                "parseTerminalDecision",
                "直接回答用户"
        );

        assertNotNull(decision);
        assertEquals("ASK_USER", ReflectionTestUtils.invokeMethod(decision, "nextAction"));
        assertEquals("请提供错误日志", ReflectionTestUtils.invokeMethod(decision, "userQuestion"));
        assertNull(invalid);
    }

    @Test
    void rejectsMultipleExceededAndRepeatedToolCalls() {
        ChatServiceImpl service = serviceWithObjectMapper();
        AssistantMessage.ToolCall first = new AssistantMessage.ToolCall(
                "1",
                "function",
                "search_article_chunks",
                "{\"query\":\"证据\"}"
        );
        AssistantMessage.ToolCall second = new AssistantMessage.ToolCall(
                "2",
                "function",
                "get_author_articles",
                "{}"
        );

        Object multiple = ReflectionTestUtils.invokeMethod(
                service,
                "validateToolCall",
                List.of(first, second),
                0,
                Set.of()
        );
        Object exceeded = ReflectionTestUtils.invokeMethod(
                service,
                "validateToolCall",
                List.of(first),
                2,
                Set.of()
        );
        Set<String> used = new HashSet<>();
        used.add("search_article_chunks:{\"query\":\"证据\"}");
        Object repeated = ReflectionTestUtils.invokeMethod(
                service,
                "validateToolCall",
                List.of(first),
                0,
                used
        );

        assertEquals("LIMIT_REACHED", ReflectionTestUtils.invokeMethod(multiple, "stopReason"));
        assertEquals("LIMIT_REACHED", ReflectionTestUtils.invokeMethod(exceeded, "stopReason"));
        assertEquals("ERROR", ReflectionTestUtils.invokeMethod(repeated, "stopReason"));
    }

    @Test
    void ignoresEmptyAndRepeatedEvidence() {
        ChatServiceImpl service = serviceWithObjectMapper();
        Set<String> visited = new HashSet<>();
        List<Object> evidences = new ArrayList<>();

        Integer first = ReflectionTestUtils.invokeMethod(
                service,
                "addEvidence",
                "search_article_chunks",
                "原文证据",
                visited,
                evidences
        );
        Integer repeated = ReflectionTestUtils.invokeMethod(
                service,
                "addEvidence",
                "search_article_chunks",
                "原文证据",
                visited,
                evidences
        );
        Integer empty = ReflectionTestUtils.invokeMethod(
                service,
                "addEvidence",
                "search_article_chunks",
                " ",
                visited,
                evidences
        );

        assertEquals(1, first);
        assertEquals(0, repeated);
        assertEquals(0, empty);
        assertEquals(1, evidences.size());
    }

    private static ChatServiceImpl serviceWithObjectMapper() {
        ChatServiceImpl service = new ChatServiceImpl();
        ReflectionTestUtils.setField(service, "objectMapper", new ObjectMapper());
        return service;
    }
}
