package io.github.grantchen2003.cdb.tx.manager.redis.writeschema;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.util.Map;

public class WriteSchemaFetcher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DynamoDbClient dynamoDbClient;

    public WriteSchemaFetcher() {
        this.dynamoDbClient = DynamoDbClient.create();
    }

    public WriteSchema fetch(String writeSchemaId) {
        final GetItemRequest request = GetItemRequest.builder()
                .tableName("write_schemas")
                .key(Map.of("id", AttributeValue.fromS(writeSchemaId)))
                .build();

        final GetItemResponse response = dynamoDbClient.getItem(request);

        if (!response.hasItem() || response.item().isEmpty()) {
            throw new IllegalStateException("Write schema not found in DynamoDB for id: " + writeSchemaId);
        }

        final AttributeValue writeSchemaJsonAttr = response.item().get("writeSchemaJson");
        if (writeSchemaJsonAttr == null || writeSchemaJsonAttr.s() == null) {
            throw new IllegalStateException("DynamoDB item missing 'writeSchemaJson' attribute for id: " + writeSchemaId);
        }

        try {
            return OBJECT_MAPPER.readValue(writeSchemaJsonAttr.s(), WriteSchema.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse write schema JSON: " + e.getMessage(), e);
        }
    }
}
