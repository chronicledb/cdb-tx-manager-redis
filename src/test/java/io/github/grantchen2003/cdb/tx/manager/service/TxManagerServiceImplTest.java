package io.github.grantchen2003.cdb.tx.manager.service;

import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleClient;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.Operation;
import io.github.grantchen2003.cdb.tx.manager.tx.Transaction;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TxManagerServiceImplTest {

    private ChronicleClient chronicleClientMock;
    private WriteSchema writeSchemaMock;
    private StreamObserver<CommitTransactionResponse> responseObserverMock;
    private TxManagerServiceImpl service;

    private static final String TABLE = "products";

    @BeforeEach
    void setUp() {
        chronicleClientMock = mock(ChronicleClient.class);
        writeSchemaMock = mock(WriteSchema.class);
        responseObserverMock = mock(StreamObserver.class);

        // set up a valid write schema that accepts "products" with a "name" string field
        WriteSchema.TableDefinition tableDef = mock(WriteSchema.TableDefinition.class);
        when(tableDef.attributeTypes()).thenReturn(Map.of("eye", "string"));
        when(writeSchemaMock.tables()).thenReturn(Map.of(TABLE, tableDef));

        service = new TxManagerServiceImpl(chronicleClientMock, writeSchemaMock);
    }

    private CommitTransactionRequest buildRequest(long seqNum, String opType, String table, String data) {
        return CommitTransactionRequest.newBuilder()
                .setExpectedSeqNum(seqNum)
                .addOperations(Operation.newBuilder()
                        .setOpType(Operation.OpType.valueOf(opType))
                        .setTable(table)
                        .setData(data)
                        .build())
                .build();
    }

    @Test
    void commitTransaction_validRequest_returnsSuccess() {
        when(chronicleClientMock.appendTx(any(Transaction.class))).thenReturn(2L);

        final CommitTransactionRequest request = buildRequest(1L, "SET", TABLE, "{\"eye\":\"some-value\"}");
        service.commitTransaction(request, responseObserverMock);

        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.SUCCESS &&
                        r.getAppliedSeqNum() == 2L
        ));
        verify(responseObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_unknownTable_returnsFailureWithoutCallingChronicle() {
        final CommitTransactionRequest request = buildRequest(1L, "SET", "unknown_table", "{\"eye\":\"val\"}");
        service.commitTransaction(request, responseObserverMock);

        verify(chronicleClientMock, never()).appendTx(any());
        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE
        ));
        verify(responseObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_invalidJson_returnsFailureWithoutCallingChronicle() {
        final CommitTransactionRequest request = buildRequest(1L, "SET", TABLE, "not-json");
        service.commitTransaction(request, responseObserverMock);

        verify(chronicleClientMock, never()).appendTx(any());
        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE
        ));
        verify(responseObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_chronicleReturnsUnexpectedSeqNum_returnsFailure() {
        // chronicle returns a seq num that doesn't match expectedSeqNum + 1
        when(chronicleClientMock.appendTx(any(Transaction.class))).thenReturn(99L);

        final CommitTransactionRequest request = buildRequest(1L, "SET", TABLE, "{\"eye\":\"some-value\"}");
        service.commitTransaction(request, responseObserverMock);

        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        r.getAppliedSeqNum() == 99L
        ));
    }
}