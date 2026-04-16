package io.github.grantchen2003.cdb.tx.manager.server;

import io.github.grantchen2003.cdb.tx.manager.service.TxManagerServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class TxManagerServer {
    private final Server grpcServer;
    private final int port;

    public TxManagerServer(int port, TxManagerServiceImpl txManagerService) {
        this.port = port;
        this.grpcServer = ServerBuilder.forPort(port)
                .addService(txManagerService)
                .build();
    }

    public void start() throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        grpcServer.start();
        System.out.println("cdb-tx-manager started on port " + port);
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    private void shutdown() {
        grpcServer.shutdown();
        System.out.println("Stopped cdb-tx-manager");
    }
}
