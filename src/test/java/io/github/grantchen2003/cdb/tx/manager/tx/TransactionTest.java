package io.github.grantchen2003.cdb.tx.manager.tx;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransactionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void serializeOperations_producesJsonArray() throws Exception {
        final List<Operation> ops = List.of(
                new Operation(Operation.OpType.SET, "products", "{\"eye\":\"some-value\"}"),
                new Operation(Operation.OpType.DELETE, "orders", "{}")
        );
        final Transaction tx = new Transaction(1L, ops);

        final String json = tx.serializeOperations();
        final var parsed = MAPPER.readTree(json);

        assertTrue(parsed.isArray());
        assertEquals(2, parsed.size());
        assertEquals("SET", parsed.get(0).get("opType").asText());
        assertEquals("products", parsed.get(0).get("table").asText());
        assertEquals("DELETE", parsed.get(1).get("opType").asText());
        assertEquals("orders", parsed.get(1).get("table").asText());
    }

    @Test
    void serializeOperations_emptyList_producesEmptyArray() throws Exception {
        final Transaction tx = new Transaction(1L, List.of());
        final String json = tx.serializeOperations();
        final var parsed = MAPPER.readTree(json);

        assertTrue(parsed.isArray());
        assertEquals(0, parsed.size());
    }
}