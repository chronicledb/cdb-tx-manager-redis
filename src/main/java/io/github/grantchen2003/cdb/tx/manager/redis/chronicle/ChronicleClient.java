package io.github.grantchen2003.cdb.tx.manager.redis.chronicle;

import io.github.grantchen2003.cdb.tx.manager.redis.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.ChronicleServiceGrpc;
import io.github.grantchen2003.cdb.tx.manager.redis.tx.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChronicleClient {

    private final ChronicleServiceGrpc.ChronicleServiceBlockingStub blockingStub;
    private final String chronicleId;

    public ChronicleClient(String host, int port, String chronicleId) {
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
                .setSeqNum(tx.expectedSeqNum())
                .setTx(tx.serializeOperations())
                .build();

        final AppendTxResponse response = blockingStub.appendTx(request);

        return response.getCommittedSeqNum();
    }
}
