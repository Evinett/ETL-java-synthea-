package com.example.syntheaetl;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvLoader {

    private final Connection connection;
    private final String schema;
    private final char delimiter;
    private final String dateFormat; // e.g., "yyyyMMdd" or "yyyy-MM-dd"

    // Helper class to store column metadata
    private static class ColumnMetadata {
        final String typeName;
        final int columnSize;

        ColumnMetadata(String typeName, int columnSize) {
            this.typeName = typeName;
            this.columnSize = columnSize;
        }
    }

    public CsvLoader(Connection connection, String schema, char delimiter, String dateFormat) {
        this.connection = connection;
        this.schema = schema;
        this.delimiter = delimiter;
        this.dateFormat = dateFormat;
    }

    public void loadAllCsvInDirectory(String directoryPath, List<String> filesToLoad) throws IOException, SQLException {
        List<Path> files;
        try (Stream<Path> stream = Files.list(Paths.get(directoryPath))) {
            files = stream
                    .filter(file -> !Files.isDirectory(file))
                    .filter(file -> {
                        String lowerCaseFileName = file.getFileName().toString().toLowerCase();
                        return filesToLoad == null || filesToLoad.stream().anyMatch(lowerCaseFileName::equalsIgnoreCase);
                    })
                    .collect(Collectors.toList());
        }

        for (Path filePath : files) {
            String fileName = filePath.getFileName().toString();
            String tableName = fileName.substring(0, fileName.lastIndexOf('.')).toLowerCase();
            System.out.println("\nProcessing file: " + fileName + " for table: " + tableName);
            loadCsvWithStaging(filePath, tableName);
        }
    }

    private void loadCsvWithStaging(Path filePath, String tableName) throws SQLException, IOException {
        String stagingTableName = "staging_" + tableName;
        String qualifiedStagingTableName = schema + "." + stagingTableName;
        String qualifiedFinalTableName = schema + "." + tableName;

        // 1. Get header from CSV and target column types from DB metadata
        Map<String, ColumnMetadata> targetColumnMetadata = getColumnMetadata(tableName);
        String[] headers;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
            String headerLine = reader.readLine();
            // Handle potential BOM at the start of the file
            if (headerLine != null && headerLine.startsWith("\uFEFF")) {
                headerLine = headerLine.substring(1);
            }
            headers = headerLine.split(String.valueOf(this.delimiter));
        }

        try (Statement stmt = connection.createStatement()) {
            // 2. Create a temporary, unlogged staging table with all TEXT columns
            String createStagingTableSql = "CREATE UNLOGGED TABLE " + qualifiedStagingTableName + " (" +
                    Stream.of(headers).map(h -> "\"" + h.toLowerCase() + "\" TEXT").collect(Collectors.joining(", ")) +
                    ")";
            stmt.execute(createStagingTableSql);

            // 3. Use the fast PostgreSQL COPY command to load raw data
            System.out.println(" - Bulk loading into staging table: " + stagingTableName);
            CopyManager copyManager = new CopyManager((BaseConnection) connection.unwrap(Connection.class));
            String copySql = String.format("COPY %s FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER '%c', NULL '')",
                    qualifiedStagingTableName, this.delimiter);

            long rowsAffected;
            try (FileReader fileReader = new FileReader(filePath.toFile())) {
                rowsAffected = copyManager.copyIn(copySql, fileReader);
            }
            System.out.println(" - Staging complete. " + rowsAffected + " rows loaded.");

            // 4. Insert from staging to final table, applying type conversions
            System.out.println(" - Transforming and inserting into final table: " + tableName);
            String insertSelectSql = generateInsertSelectSql(qualifiedFinalTableName, qualifiedStagingTableName, headers, targetColumnMetadata);
            int insertedRows = stmt.executeUpdate(insertSelectSql);
            System.out.println(" - Success. Total rows inserted: " + insertedRows);

        } finally {
            // 5. Always drop the staging table
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + qualifiedStagingTableName);
            }
        }
    }

    private Map<String, ColumnMetadata> getColumnMetadata(String tableName) throws SQLException {
        Map<String, ColumnMetadata> columnMetadataMap = new HashMap<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, this.schema, tableName.toLowerCase(), null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME").toLowerCase();
                String typeName = rs.getString("TYPE_NAME").toLowerCase();
                int columnSize = rs.getInt("COLUMN_SIZE");
                columnMetadataMap.put(columnName, new ColumnMetadata(typeName, columnSize));
            }
        }
        return columnMetadataMap;
    }

    private String generateInsertSelectSql(String finalTable, String stagingTable, String[] headers, Map<String, ColumnMetadata> columnMetadata) {
        String selectExpressions = Stream.of(headers)
                .map(String::toLowerCase)
                .map(header -> {
                    ColumnMetadata meta = columnMetadata.get(header);
                    String type = (meta != null) ? meta.typeName : "varchar";
                    int size = (meta != null) ? meta.columnSize : 0;

                    String col = "\"" + header + "\"";
                    // Handle empty strings as NULL before casting
                    String expression = "NULLIF(TRIM(" + col + "), '')";

                    if (type.startsWith("int") || type.equals("bigint")) {
                        return "CAST(" + expression + " AS " + type + ")";
                    } else if (type.equals("numeric") || type.equals("decimal")) {
                        return "CAST(" + expression + " AS NUMERIC)";
                    } else if (type.equals("date")) {
                        // Use TO_DATE for robust date parsing. Convert Java format to PostgreSQL format.
                        String postgresDateFormat = this.dateFormat.replace("yyyy", "YYYY").replace("MM", "MM").replace("dd", "DD");
                        return "TO_DATE(" + expression + ", '" + postgresDateFormat + "')";
                    } else {
                        // Default to TEXT/VARCHAR, but apply SUBSTRING if size is known and > 0 for varchar/char types
                        if (size > 0 && (type.equals("varchar") || type.equals("bpchar"))) {
                            return "SUBSTRING(" + expression + " FROM 1 FOR " + size + ")";
                        }
                        return expression;
                    }
                })
                .collect(Collectors.joining(", "));

        String columnNames = Stream.of(headers)
                .map(h -> "\"" + h.toLowerCase() + "\"")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) SELECT %s FROM %s", finalTable, columnNames, selectExpressions, stagingTable);
    }
}