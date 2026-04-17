package io.github.grantchen2003.cdb.tx.manager.writeschema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public record WriteSchema(
        Map<String, TypeDefinition> types,
        Map<String, TableDefinition> tables
) {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setDefaultPropertyInclusion(JsonInclude.Value.construct(
                    JsonInclude.Include.NON_NULL,
                    JsonInclude.Include.NON_NULL
            ));

    public static WriteSchema fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, WriteSchema.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid WriteSchema JSON provided", e);
        }
    }

    public record TypeDefinition(
            String variant,
            String charset,
            List<Long> size,
            List<Long> range,
            List<Object> values,
            Long precision,
            List<Long> scale
    ) {}

    public record TableDefinition(
            List<String> primaryKey,
            List<List<String>> keys,
            List<String> requiredAttributes,
            List<List<String>> queryFamilies,
            Map<String, String> attributeTypes,
            String updateSNAttribute
    ) {}
}