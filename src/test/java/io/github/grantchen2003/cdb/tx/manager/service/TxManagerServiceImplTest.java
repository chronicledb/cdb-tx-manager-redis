package io.github.grantchen2003.cdb.tx.manager.service;

import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleServiceClient;
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

    private ChronicleServiceClient chronicleServiceClientMock;
    private WriteSchema writeSchemaMock;
    private StreamObserver<CommitTransactionResponse> responseObserverMock;
    private TxManagerServiceImpl service;

    private static final String TABLE = "products";

    @BeforeEach
    void setUp() {
        chronicleServiceClientMock = mock(ChronicleServiceClient.class);
        writeSchemaMock = mock(WriteSchema.class);
        responseObserverMock = mock(StreamObserver.class);

        WriteSchema.TableDefinition tableDef = mock(WriteSchema.TableDefinition.class);
        when(tableDef.attributeTypes()).thenReturn(Map.of("eye", "string"));
        when(writeSchemaMock.tables()).thenReturn(Map.of(TABLE, tableDef));

        service = new TxManagerServiceImpl(chronicleServiceClientMock, writeSchemaMock);
    }

    private CommitTransactionRequest buildRequest(long seqNum, String opType, String table, String data) {
        return CommitTransactionRequest.newBuilder()
                .setSeqNum(seqNum)
                .addOperations(Operation.newBuilder()
                        .setOpType(Operation.OpType.valueOf(opType))
                        .setTable(table)
                        .setData(data)
                        .build())
                .build();
    }

    @Test
    void commitTransaction_validRequest_returnsSuccess() {
        when(chronicleServiceClientMock.appendTx(any(Transaction.class))).thenReturn(new ChronicleServiceClient.TxAppendResult(true, 2L, ""));

        final CommitTransactionRequest request = buildRequest(1L, "PUT", TABLE, "{\"eye\":\"some-value\"}");
        service.commitTransaction(request, responseObserverMock);

        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.SUCCESS &&
                        r.getCommittedSeqNum() == 2L &&
                        r.getErrorMessage().isEmpty()
        ));
        verify(responseObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_unknownTable_returnsFailureWithError() {
        final CommitTransactionRequest request = buildRequest(1L, "PUT", "unknown_table", "{\"eye\":\"val\"}");
        service.commitTransaction(request, responseObserverMock);

        verify(chronicleServiceClientMock, never()).appendTx(any());
        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        r.getErrorMessage().contains("Unknown table")
        ));
        verify(responseObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_invalidJson_returnsFailureWithError() {
        final CommitTransactionRequest request = buildRequest(1L, "PUT", TABLE, "not-json");
        service.commitTransaction(request, responseObserverMock);

        verify(chronicleServiceClientMock, never()).appendTx(any());
        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        !r.getErrorMessage().isEmpty()
        ));
        verify(responseObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_chronicleReturnsUnexpectedSeqNum_returnsFailureWithMessage() {
        long expected = 1L;
        long returned = 99L;
        when(chronicleServiceClientMock.appendTx(any(Transaction.class))).thenReturn(new ChronicleServiceClient.TxAppendResult(false, 2L, ""));

        final CommitTransactionRequest request = buildRequest(expected, "PUT", TABLE, "{\"eye\":\"some-value\"}");
        service.commitTransaction(request, responseObserverMock);

        verify(responseObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        r.getCommittedSeqNum() == returned &&
                        r.getErrorMessage().equals("Sequence number mismatch: expected " + (expected + 1) + ", got " + returned)
        ));
        verify(responseObserverMock).onCompleted();
    }
}