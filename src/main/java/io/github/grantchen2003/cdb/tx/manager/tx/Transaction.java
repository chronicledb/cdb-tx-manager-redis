package io.github.grantchen2003.cdb.tx.manager.tx;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public record Transaction(long expectedSeqNum, List<Operation> operations) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String serializeOperations() {
        try {
            return MAPPER.writeValueAsString(operations);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize operations", e);
        }
    }
}