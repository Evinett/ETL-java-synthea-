package com.example.syntheaetl;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

public class CdmManager {

    private final Connection connection;
    private final String cdmSchema;

    public CdmManager(Connection connection, String cdmSchema) {
        this.connection = connection;
        this.cdmSchema = cdmSchema;
    }

    public void createCdmTables(String cdmVersion, String dbms) throws Exception {
        System.out.println("\n--- Creating OMOP CDM v" + cdmVersion + " Tables for " + dbms + " ---");

        String ddlUrl = String.format(
            "https://raw.githubusercontent.com/OHDSI/CommonDataModel/v%s/inst/ddl/%s/%s/OMOPCDM_%s_%s_ddl.sql",
            cdmVersion, cdmVersion, dbms, dbms, cdmVersion
        );

        System.out.println("Downloading DDL from: " + ddlUrl);
        String ddlSql;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(ddlUrl);
            ddlSql = httpClient.execute(request, response -> EntityUtils.toString(response.getEntity()));
        }

        if (ddlSql == null || ddlSql.trim().isEmpty()) {
            throw new RuntimeException("Failed to download DDL script from GitHub. URL: " + ddlUrl);
        }

        // Replace the schema placeholder used in the official DDL scripts
        ddlSql = ddlSql.replaceAll("@cdmDatabaseSchema", this.cdmSchema);

        System.out.println("Executing DDL script to create CDM tables...");
        try (Statement stmt = connection.createStatement()) {
            // The DDL script contains multiple statements; execute it as a single batch.
            stmt.execute(ddlSql);
        }
        System.out.println("--- OMOP CDM Tables created successfully ---\n");
    }
}