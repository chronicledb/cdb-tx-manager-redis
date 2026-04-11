package io.github.grantchen2003.cdb.tx.manager.redis.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.CommitTransactionRequest;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.CommitTransactionResponse;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.Operation;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.TxManagerRedisServiceGrpc;
import io.github.grantchen2003.cdb.tx.manager.redis.writeschema.WriteSchema;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;

public class TxManagerRedisServiceImpl extends TxManagerRedisServiceGrpc.TxManagerRedisServiceImplBase {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final WriteSchema writeSchema;

    public TxManagerRedisServiceImpl(WriteSchema writeSchema) {
        this.writeSchema = writeSchema;
    }

    @Override
    public void commitTransaction(CommitTransactionRequest request, StreamObserver<CommitTransactionResponse> responseObserver) {
        final long expectedSeqNum = request.getExpectedSeqNum();
        final List<Operation> operations = request.getOperationsList();

        for (final Operation op : operations) {
            final String validationError = validateAgainstWriteSchema(op);
            if (validationError != null) {
                final CommitTransactionResponse failureResponse = CommitTransactionResponse.newBuilder()
                        .setStatus(CommitTransactionResponse.Code.FAILURE)
                        .setAppliedSeqNum(expectedSeqNum)
                        .build();
                responseObserver.onNext(failureResponse);
                responseObserver.onCompleted();
                return;
            }
        }

        final CommitTransactionResponse response = CommitTransactionResponse.newBuilder()
                .setStatus(CommitTransactionResponse.Code.SUCCESS)
                .setAppliedSeqNum(expectedSeqNum + 1)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private String validateAgainstWriteSchema(Operation op) {
        final String tableName = op.getTable();
        final Map<String, WriteSchema.TableDefinition> tables = writeSchema.tables();

        if (!tables.containsKey(tableName)) {
            return String.format("Unknown table '%s'. Allowed tables: %s", tableName, tables.keySet());
        }

        final WriteSchema.TableDefinition tableDef = tables.get(tableName);
        final Map<String, String> allowedAttributes = tableDef.attributeTypes();

        final Map<String, Object> data = parseData(op.getData());
        if (data == null) {
            return String.format("Failed to parse data for operation on table '%s': not valid JSON", tableName);
        }

        for (final String field : data.keySet()) {
            if (!allowedAttributes.containsKey(field)) {
                return String.format(
                        "Unknown attribute '%s' on table '%s'. Allowed attributes: %s",
                        field, tableName, allowedAttributes.keySet()
                );
            }
        }

        return null;
    }

    private Map<String, Object> parseData(String dataJson) {
        try {
            return OBJECT_MAPPER.readValue(dataJson, new TypeReference<>() {});
        } catch (Exception e) {
            return null;
        }
    }
}
