package io.github.grantchen2003.cdb.tx.manager.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleServiceClient;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.TxManagerServiceGrpc;
import io.github.grantchen2003.cdb.tx.manager.tx.Operation;
import io.github.grantchen2003.cdb.tx.manager.tx.Transaction;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class TxManagerServiceImpl extends TxManagerServiceGrpc.TxManagerServiceImplBase {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ChronicleServiceClient chronicleServiceClient;
    private final WriteSchema writeSchema;

    public TxManagerServiceImpl(ChronicleServiceClient chronicleServiceClient, WriteSchema writeSchema) {
        this.chronicleServiceClient = chronicleServiceClient;
        this.writeSchema = writeSchema;
    }

    @Override
    public void commitTransaction(CommitTransactionRequest request, StreamObserver<CommitTransactionResponse> responseObserver) {
        final Transaction tx = new Transaction(
                request.getSeqNum(),
                request.getOperationsList().stream()
                        .map(op -> new Operation(
                                Operation.OpType.valueOf(op.getOpType().name()),
                                op.getTable(),
                                op.getData()
                        ))
                        .toList()
        );

        for (final Operation op : tx.operations()) {
            final String validationError = validateAgainstWriteSchema(op);
            if (validationError != null) {
                final CommitTransactionResponse failureResponse = CommitTransactionResponse.newBuilder()
                        .setStatus(CommitTransactionResponse.Code.FAILURE)
                        .setCommittedSeqNum(tx.seqNum())
                        .setErrorMessage(validationError)
                        .build();
                responseObserver.onNext(failureResponse);
                responseObserver.onCompleted();
                return;
            }
        }

        final ChronicleServiceClient.TxAppendResult appendTxResult = chronicleServiceClient.appendTx(tx);

        final CommitTransactionResponse response;
        if (appendTxResult.success()) {
            response = CommitTransactionResponse.newBuilder()
                    .setStatus(CommitTransactionResponse.Code.SUCCESS)
                    .setCommittedSeqNum(appendTxResult.committedSeqNum())
                    .build();
        } else {
            response = CommitTransactionResponse.newBuilder()
                    .setStatus(CommitTransactionResponse.Code.FAILURE)
                    .setCommittedSeqNum(appendTxResult.committedSeqNum())
                    .setErrorMessage(appendTxResult.errorMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private String validateAgainstWriteSchema(Operation op) {
        final String tableName = op.table();
        final Map<String, WriteSchema.TableDefinition> tables = writeSchema.tables();

        if (!tables.containsKey(tableName)) {
            return String.format("Unknown table '%s'. Allowed tables: %s", tableName, tables.keySet());
        }

        final WriteSchema.TableDefinition tableDef = tables.get(tableName);
        final Map<String, String> allowedAttributes = tableDef.attributeTypes();

        final Map<String, Object> data = parseData(op.data());
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
