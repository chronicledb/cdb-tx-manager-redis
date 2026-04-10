package io.github.grantchen2003.cdb.tx.manager.redis;

import io.github.grantchen2003.cdb.tx.manager.redis.config.EnvConfig;
import io.github.grantchen2003.cdb.tx.manager.redis.server.TxManagerRedisServer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final int port = Integer.parseInt(EnvConfig.get("TX_MANAGER_REDIS_PORT"));

        final TxManagerRedisServer server = new TxManagerRedisServer(port);
        server.start();
        server.awaitTermination();
    }
}