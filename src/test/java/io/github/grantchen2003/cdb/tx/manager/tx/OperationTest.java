package io.github.grantchen2003.cdb.tx.manager.tx;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OperationTest {

    @Test
    void serialize_producesExpectedJson() throws Exception {
        final Operation op = new Operation(Operation.OpType.PUT, "products", "{\"eye\":\"some-value\"}");
        final String json = op.serialize();

        assertTrue(json.contains("\"opType\":\"PUT\""));
        assertTrue(json.contains("\"table\":\"products\""));
        assertTrue(json.contains("\"data\""));
    }

    @Test
    void deserialize_roundTrip() {
        final Operation original = new Operation(Operation.OpType.PUT, "products", "{\"eye\":\"some-value\"}");
        final String json = original.serialize();
        final Operation deserialized = Operation.deserialize(json);

        assertEquals(original.opType(), deserialized.opType());
        assertEquals(original.table(), deserialized.table());
        assertEquals(original.data(), deserialized.data());
    }

    @Test
    void deserialize_deleteOpType() {
        final Operation original = new Operation(Operation.OpType.DELETE, "products", "{}");
        final Operation deserialized = Operation.deserialize(original.serialize());

        assertEquals(Operation.OpType.DELETE, deserialized.opType());
    }

    @Test
    void deserialize_invalidJson_throwsRuntimeException() {
        assertThrows(RuntimeException.class, () -> Operation.deserialize("not-json"));
    }
}