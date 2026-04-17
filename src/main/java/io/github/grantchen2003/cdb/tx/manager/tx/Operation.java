package io.github.grantchen2003.cdb.tx.manager.tx;

import com.fasterxml.jackson.databind.ObjectMapper;

public record Operation(
        OpType opType,
        String table,
        String data
) {
    public enum OpType { PUT, DELETE }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String serialize() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Operation", e);
        }
    }

    public static Operation deserialize(String json) {
        try {
            return MAPPER.readValue(json, Operation.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Operation", e);
        }
    }
}