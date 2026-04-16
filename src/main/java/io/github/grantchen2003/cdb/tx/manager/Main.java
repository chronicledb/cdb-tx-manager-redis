package io.github.grantchen2003.cdb.tx.manager;

import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleServiceClient;
import io.github.grantchen2003.cdb.tx.manager.config.EnvConfig;
import io.github.grantchen2003.cdb.tx.manager.server.TxManagerServer;
import io.github.grantchen2003.cdb.tx.manager.service.TxManagerServiceImpl;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final String chronicleId = EnvConfig.get("CHRONICLE_ID");
        final String chronicleServiceIp = EnvConfig.get("CHRONICLE_SERVICE_IP");
        final int chronicleServicePort = Integer.parseInt(EnvConfig.get("CHRONICLE_SERVICE_PORT"));
        final int txManagerPort = Integer.parseInt(EnvConfig.get("TX_MANAGER_PORT"));
        final String writeSchemaJson = EnvConfig.get("WRITE_SCHEMA_JSON");

        final ChronicleServiceClient chronicleServiceClient = new ChronicleServiceClient(chronicleServiceIp, chronicleServicePort, chronicleId);

        final WriteSchema writeSchema = WriteSchema.fromJson(writeSchemaJson);

        final TxManagerServiceImpl txManagerService = new TxManagerServiceImpl(chronicleServiceClient, writeSchema);

        final TxManagerServer server = new TxManagerServer(txManagerPort, txManagerService);

        server.start();
        server.awaitTermination();
    }
}