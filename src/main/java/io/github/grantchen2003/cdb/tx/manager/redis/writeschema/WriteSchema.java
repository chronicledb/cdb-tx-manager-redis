package io.github.grantchen2003.cdb.tx.manager.redis.writeschema;

import java.util.List;
import java.util.Map;

public record WriteSchema(
        Map<String, TypeDefinition> types,
        Map<String, TableDefinition> tables
) {
    public record TypeDefinition(String variant) {}
    public record TableDefinition(
            List<String> primaryKey,
            Map<String, String> attributeTypes
    ) {}
}