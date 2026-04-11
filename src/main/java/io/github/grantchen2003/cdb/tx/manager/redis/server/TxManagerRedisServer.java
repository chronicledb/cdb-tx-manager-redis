package io.github.grantchen2003.cdb.tx.manager.redis.server;

import io.github.grantchen2003.cdb.tx.manager.redis.service.TxManagerRedisServiceImpl;
import io.github.grantchen2003.cdb.tx.manager.redis.writeschema.WriteSchema;
import io.github.grantchen2003.cdb.tx.manager.redis.writeschema.WriteSchemaFetcher;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class TxManagerRedisServer {
    private final Server grpcServer;
    private final int port;

    public TxManagerRedisServer(int port, String writeSchemaId) {
        this.port = port;

        final WriteSchema writeSchema = fetchWriteSchema(writeSchemaId);

        this.grpcServer = ServerBuilder.forPort(port)
                .addService(new TxManagerRedisServiceImpl(writeSchema))
                .build();
    }

    public void start() throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        grpcServer.start();
        System.out.println("cdb-tx-manager-redis started on port " + port);
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    private void shutdown() {
        grpcServer.shutdown();
        System.out.println("Stopped cdb-tx-manager-redis");
    }

    private WriteSchema fetchWriteSchema(String writeSchemaId) {
        System.out.println("Fetching write schema for id: " + writeSchemaId);
        final WriteSchema schema = new WriteSchemaFetcher().fetch(writeSchemaId);
        System.out.println("Write schema loaded: " + schema.tables().keySet());
        return schema;
    }
}
