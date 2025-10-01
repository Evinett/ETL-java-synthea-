package com.example.syntheaetl;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlFileRunner {

    public static void runSqlFile(Connection conn, String filePath, Map<String, String> replacements) {
        System.out.println("Running SQL file: " + filePath);
        try {
            String sql = new String(Files.readAllBytes(Paths.get(filePath)));
            executeSql(conn, sql, replacements);
            System.out.println("Successfully executed SQL from: " + filePath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to run SQL file: " + filePath, e);
        }
    }

    public static void runSqlFromUrl(Connection conn, String url, Map<String, String> replacements) throws Exception {
        String sql;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            sql = httpClient.execute(request, response -> EntityUtils.toString(response.getEntity()));
        }
        executeSql(conn, sql, replacements);
    }

    private static void executeSql(Connection conn, String sql, Map<String, String> replacements) throws SQLException {
        String finalSql = sql;
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            // The placeholders in the SQL are like @cdm_schema
            finalSql = finalSql.replace("@" + entry.getKey(), entry.getValue());
        }

        // Use a more robust splitter that handles semicolons inside quotes and comments
        List<String> individualSqls = splitSqlScript(finalSql);

        try (Statement stmt = conn.createStatement()) {
            // Set a timeout for SQL script execution to prevent indefinite hangs.
            stmt.setQueryTimeout(300); // 5 minutes, as some data loading scripts can be long.
            for (String singleSql : individualSqls) {
                if (!singleSql.trim().isEmpty()) {
                    stmt.execute(singleSql);
                }
            }
        }
    }

    /**
     * Splits a SQL script into individual statements, respecting semicolons within
     * single-quoted strings and ignoring line comments.
     *
     * @param script The SQL script content.
     * @return A list of individual SQL statements.
     */
    private static List<String> splitSqlScript(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inStringLiteral = false;
        boolean inLineComment = false;

        for (int i = 0; i < script.length(); i++) {
            char c = script.charAt(i);

            if (inLineComment) {
                if (c == '\n') inLineComment = false;
            } else {
                if (c == '-' && i + 1 < script.length() && script.charAt(i + 1) == '-') {
                    inLineComment = true;
                } else if (c == '\'') {
                    inStringLiteral = !inStringLiteral;
                } else if (c == ';' && !inStringLiteral) {
                    statements.add(sb.toString());
                    sb.setLength(0); // Reset for the next statement
                    continue;
                }
            }
            sb.append(c);
        }

        if (!sb.toString().trim().isEmpty()) {
            statements.add(sb.toString());
        }
        return statements;
    }
}