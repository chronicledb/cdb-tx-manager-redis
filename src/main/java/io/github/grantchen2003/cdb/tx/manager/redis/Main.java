package io.github.grantchen2003.cdb.tx.manager.redis;

import io.github.grantchen2003.cdb.tx.manager.redis.chronicle.ChronicleClient;
import io.github.grantchen2003.cdb.tx.manager.redis.config.EnvConfig;
import io.github.grantchen2003.cdb.tx.manager.redis.server.TxManagerRedisServer;
import io.github.grantchen2003.cdb.tx.manager.redis.service.TxManagerRedisServiceImpl;
import io.github.grantchen2003.cdb.tx.manager.redis.writeschema.WriteSchema;
import io.github.grantchen2003.cdb.tx.manager.redis.writeschema.WriteSchemaFetcher;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final String chronicleId = EnvConfig.get("CHRONICLE_ID");
        final String chronicleHost = EnvConfig.get("CHRONICLE_HOST");
        final int chroniclePort = Integer.parseInt(EnvConfig.get("CHRONICLE_PORT"));
        final int txManagerRedisPort = Integer.parseInt(EnvConfig.get("TX_MANAGER_REDIS_PORT"));
        final String writeSchemaId = EnvConfig.get("WRITE_SCHEMA_ID");

        final ChronicleClient chronicleClient = new ChronicleClient(chronicleHost, chroniclePort, chronicleId);

        final WriteSchemaFetcher writeSchemaFetcher = new WriteSchemaFetcher();
        final WriteSchema writeSchema = writeSchemaFetcher.fetch(writeSchemaId);

        final TxManagerRedisServiceImpl txManagerRedisService = new TxManagerRedisServiceImpl(chronicleClient, writeSchema);

        final TxManagerRedisServer server = new TxManagerRedisServer(txManagerRedisPort, txManagerRedisService);

        server.start();
        server.awaitTermination();
    }
}