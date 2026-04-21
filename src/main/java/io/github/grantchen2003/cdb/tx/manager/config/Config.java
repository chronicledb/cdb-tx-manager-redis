package io.github.grantchen2003.cdb.tx.manager.config;

import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleServiceClient;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Value("${cdb.chronicle.service.ip}")
    private String chronicleServiceIp;

    @Value("${cdb.chronicle.service.port}")
    private int chronicleServicePort;

    @Value("${chronicle-id}")
    private String chronicleId;

    @Value("${write-schema-json}")
    private String writeSchemaJson;

    @Bean
    public ChronicleServiceClient chronicleServiceClient() {
        return new ChronicleServiceClient(chronicleServiceIp, chronicleServicePort, chronicleId);
    }

    @Bean
    public WriteSchema writeSchema() {
        return WriteSchema.fromJson(writeSchemaJson);
    }
}