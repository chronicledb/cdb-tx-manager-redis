package io.github.grantchen2003.cdb.tx.manager.chronicle;

import io.github.grantchen2003.cdb.tx.manager.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.ChronicleServiceGrpc;
import io.github.grantchen2003.cdb.tx.manager.tx.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChronicleServiceClient {

    private final ChronicleServiceGrpc.ChronicleServiceBlockingStub blockingStub;
    private final String chronicleId;

    public ChronicleServiceClient(String host, int port, String chronicleId) {
        final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()        // TODO: remove this and configure TLS for production
                .build();

        this.blockingStub = ChronicleServiceGrpc.newBlockingStub(channel);
        this.chronicleId = chronicleId;
    }

    public long appendTx(Transaction tx) {
        final AppendTxRequest request = AppendTxRequest.newBuilder()
                .setChronicleId(chronicleId)
                .setSeqNum(tx.seqNum())
                .setTx(tx.serializeOperations())
                .build();

        final AppendTxResponse response = blockingStub.appendTx(request);

        return response.getCommittedSeqNum();
    }
}
