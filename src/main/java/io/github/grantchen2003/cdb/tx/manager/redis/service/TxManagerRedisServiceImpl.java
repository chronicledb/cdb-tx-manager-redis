package io.github.grantchen2003.cdb.tx.manager.redis.service;

import io.github.grantchen2003.cdb.tx.manager.redis.grpc.CommitTransactionRequest;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.CommitTransactionResponse;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.Operation;
import io.github.grantchen2003.cdb.tx.manager.redis.grpc.TxManagerRedisServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class TxManagerRedisServiceImpl extends TxManagerRedisServiceGrpc.TxManagerRedisServiceImplBase {

    @Override
    public void commitTransaction(CommitTransactionRequest request, StreamObserver<CommitTransactionResponse> responseObserver) {
        final long expectedSeqNum = request.getExpectedSeqNum();
        final List<Operation> operations = request.getOperationsList();

        System.out.println(expectedSeqNum);

        for (final Operation op : operations) {
            System.out.println(op.getOpType());
            System.out.println(op.getTable());
            System.out.println(op.getData());
        }

        final CommitTransactionResponse response = CommitTransactionResponse.newBuilder()
                .setStatus(CommitTransactionResponse.Code.SUCCESS)
                .setAppliedSeqNum(expectedSeqNum + 1)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
