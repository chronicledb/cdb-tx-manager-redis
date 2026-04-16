package io.github.grantchen2003.cdb.tx.manager.chronicle;

import io.github.grantchen2003.cdb.tx.manager.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.ChronicleServiceGrpc;
import io.github.grantchen2003.cdb.tx.manager.tx.Operation;
import io.github.grantchen2003.cdb.tx.manager.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChronicleClientTest {

    private ChronicleServiceGrpc.ChronicleServiceBlockingStub stubMock;
    private ChronicleClient client;

    @BeforeEach
    void setUp() throws Exception {
        stubMock = mock(ChronicleServiceGrpc.ChronicleServiceBlockingStub.class);

        // inject mock stub via reflection since the channel is built in the constructor
        client = new ChronicleClient("localhost", 50051, "chronicle-1");
        final var field = ChronicleClient.class.getDeclaredField("blockingStub");
        field.setAccessible(true);
        field.set(client, stubMock);
    }

    @Test
    void appendTx_returnsCommittedSeqNum() {
        final Transaction tx = new Transaction(1L, List.of(
                new Operation(Operation.OpType.SET, "products", "{\"eye\":\"some-value\"}")
        ));

        when(stubMock.appendTx(any())).thenReturn(
                AppendTxResponse.newBuilder().setCommittedSeqNum(2L).build()
        );

        final long result = client.appendTx(tx);

        assertEquals(2L, result);
    }

    @Test
    void appendTx_sendsCorrectRequest() {
        final Transaction tx = new Transaction(1L, List.of(
                new Operation(Operation.OpType.SET, "products", "{\"eye\":\"some-value\"}")
        ));

        when(stubMock.appendTx(any())).thenReturn(
                AppendTxResponse.newBuilder().setCommittedSeqNum(2L).build()
        );

        client.appendTx(tx);

        ArgumentCaptor<AppendTxRequest> captor = ArgumentCaptor.forClass(AppendTxRequest.class);
        verify(stubMock).appendTx(captor.capture());

        AppendTxRequest sentRequest = captor.getValue();
        assertEquals("chronicle-1", sentRequest.getChronicleId());
        assertEquals(1L, sentRequest.getSeqNum());
        assertFalse(sentRequest.getTx().isEmpty());
    }
}