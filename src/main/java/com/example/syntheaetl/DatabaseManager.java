package com.example.syntheaetl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseManager implements AutoCloseable {

    private final HikariDataSource dataSource;

    public DatabaseManager() {
        HikariConfig config = new HikariConfig();
        // Corresponds to createConnectionDetails() in R
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/Syn_CDM");
        config.setUsername("rward");
        config.setPassword("admin"); // For production, use a secure way to handle passwords
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}