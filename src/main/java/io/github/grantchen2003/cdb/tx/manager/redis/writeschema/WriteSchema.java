package io.github.grantchen2003.cdb.tx.manager.redis.writeschema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public record WriteSchema(
        Map<String, TypeDefinition> types,
        Map<String, TableDefinition> tables
) {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static WriteSchema fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, WriteSchema.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid WriteSchema JSON provided", e);
        }
    }

    public record TypeDefinition(String variant) {}
    public record TableDefinition(
            List<String> primaryKey,
            Map<String, String> attributeTypes
    ) {}
}