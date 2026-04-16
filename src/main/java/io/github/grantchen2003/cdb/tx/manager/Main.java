package io.github.grantchen2003.cdb.tx.manager;

import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleClient;
import io.github.grantchen2003.cdb.tx.manager.config.EnvConfig;
import io.github.grantchen2003.cdb.tx.manager.server.TxManagerServer;
import io.github.grantchen2003.cdb.tx.manager.service.TxManagerServiceImpl;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final String chronicleId = EnvConfig.get("CHRONICLE_ID");
        final String chronicleHost = EnvConfig.get("CHRONICLE_HOST");
        final int chroniclePort = Integer.parseInt(EnvConfig.get("CHRONICLE_PORT"));
        final int txManagerPort = Integer.parseInt(EnvConfig.get("TX_MANAGER_PORT"));
        final String writeSchemaJson = EnvConfig.get("WRITE_SCHEMA_JSON");

        final ChronicleClient chronicleClient = new ChronicleClient(chronicleHost, chroniclePort, chronicleId);

        final WriteSchema writeSchema = WriteSchema.fromJson(writeSchemaJson);

        final TxManagerServiceImpl txManagerService = new TxManagerServiceImpl(chronicleClient, writeSchema);

        final TxManagerServer server = new TxManagerServer(txManagerPort, txManagerService);

        server.start();
        server.awaitTermination();
    }
}