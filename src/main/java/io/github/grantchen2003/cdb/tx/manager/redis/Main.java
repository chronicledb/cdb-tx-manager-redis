package io.github.grantchen2003.cdb.tx.manager.redis;

import io.github.grantchen2003.cdb.tx.manager.redis.config.EnvConfig;
import io.github.grantchen2003.cdb.tx.manager.redis.server.TxManagerRedisServer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final int port = Integer.parseInt(EnvConfig.get("TX_MANAGER_REDIS_PORT"));
        final String writeSchemaId = EnvConfig.get("WRITE_SCHEMA_ID");

        final TxManagerRedisServer server = new TxManagerRedisServer(port, writeSchemaId);
        server.start();
        server.awaitTermination();
    }
}